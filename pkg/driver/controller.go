package driver

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/leaseweb/cloudstack-csi-driver/pkg/cloud"
	"github.com/leaseweb/cloudstack-csi-driver/pkg/util"
)

// onlyVolumeCapAccessMode is the only volume capability access
// mode possible for CloudStack: SINGLE_NODE_WRITER, since a
// CloudStack volume can only be attached to a single node at
// any given time.
var onlyVolumeCapAccessMode = csi.VolumeCapability_AccessMode{
	Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
}

type controllerServer struct {
	csi.UnimplementedControllerServer
	connector   cloud.Interface
	volumeLocks *util.VolumeLocks
}

// NewControllerServer creates a new Controller gRPC server.
func NewControllerServer(connector cloud.Interface) csi.ControllerServer {
	return &controllerServer{
		connector:   connector,
		volumeLocks: util.NewVolumeLocks(),
	}
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {

	// Check arguments

	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume name missing in request")
	}
	name := req.GetName()

	volCaps := req.GetVolumeCapabilities()
	if len(volCaps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities missing in request")
	}
	if !isValidVolumeCapabilities(volCaps) {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities not supported. Only SINGLE_NODE_WRITER supported.")
	}

	if req.GetParameters() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume parameters missing in request")
	}
	diskOfferingID := req.GetParameters()[DiskOfferingKey]
	if diskOfferingID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Missing parameter %v", DiskOfferingKey)
	}

	if acquired := cs.volumeLocks.TryAcquire(name); !acquired {
		ctxzap.Extract(ctx).Sugar().Errorf(util.VolumeOperationAlreadyExistsFmt, name)
		return nil, status.Errorf(codes.Aborted, util.VolumeOperationAlreadyExistsFmt, name)
	}
	defer cs.volumeLocks.Release(name)

	// Check if a volume with that name already exists
	if vol, err := cs.connector.GetVolumeByName(ctx, name); err == cloud.ErrNotFound {
		// The volume does not exist
	} else if err != nil {
		// Error with CloudStack
		return nil, status.Errorf(codes.Internal, "CloudStack error: %v", err)
	} else {
		// The volume exists. Check if it suits the request.
		if ok, message := checkVolumeSuitable(vol, diskOfferingID, req.GetCapacityRange(), req.GetAccessibilityRequirements()); !ok {
			return nil, status.Errorf(codes.AlreadyExists, "Volume %v already exists but does not satisfy request: %s", name, message)
		}
		// Existing volume is ok
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      vol.ID,
				CapacityBytes: vol.Size,
				VolumeContext: req.GetParameters(),
				// ContentSource: req.GetVolumeContentSource(), TODO: snapshot support
				AccessibleTopology: []*csi.Topology{
					Topology{ZoneID: vol.ZoneID}.ToCSI(),
				},
			},
		}, nil
	}

	// We have to create the volume

	// Determine volume size using requested capacity range
	sizeInGB, err := determineSize(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Determine zone using topology constraints
	var zoneID string
	topologyRequirement := req.GetAccessibilityRequirements()
	if topologyRequirement == nil || topologyRequirement.GetRequisite() == nil {
		// No topology requirement. Use random zone
		zones, err := cs.connector.ListZonesID(ctx)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		n := len(zones)
		if n == 0 {
			return nil, status.Error(codes.Internal, "No zone available")
		}
		zoneID = zones[rand.Intn(n)]
	} else {
		reqTopology := topologyRequirement.GetRequisite()
		if len(reqTopology) > 1 {
			return nil, status.Error(codes.InvalidArgument, "Too many topology requirements")
		}
		t, err := NewTopology(reqTopology[0])
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "Cannot parse topology requirements")
		}
		zoneID = t.ZoneID
	}

	ctxzap.Extract(ctx).Sugar().Infow("Creating new volume",
		"name", name,
		"size", sizeInGB,
		"offering", diskOfferingID,
		"zone", zoneID,
	)

	volID, err := cs.connector.CreateVolume(ctx, diskOfferingID, zoneID, name, sizeInGB)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Cannot create volume %s: %v", name, err.Error())
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volID,
			CapacityBytes: util.GigaBytesToBytes(sizeInGB),
			VolumeContext: req.GetParameters(),
			// ContentSource: req.GetVolumeContentSource(), TODO: snapshot support
			AccessibleTopology: []*csi.Topology{
				Topology{ZoneID: zoneID}.ToCSI(),
			},
		},
	}, nil
}

func checkVolumeSuitable(vol *cloud.Volume,
	diskOfferingID string, capRange *csi.CapacityRange, topologyRequirement *csi.TopologyRequirement) (bool, string) {

	if vol.DiskOfferingID != diskOfferingID {
		return false, fmt.Sprintf("Disk offering %s; requested disk offering %s", vol.DiskOfferingID, diskOfferingID)
	}

	if capRange != nil {
		if capRange.GetLimitBytes() > 0 && vol.Size > capRange.GetLimitBytes() {
			return false, fmt.Sprintf("Disk size %v bytes > requested limit size %v bytes", vol.Size, capRange.GetLimitBytes())
		}
		if capRange.GetRequiredBytes() > 0 && vol.Size < capRange.GetRequiredBytes() {
			return false, fmt.Sprintf("Disk size %v bytes < requested required size %v bytes", vol.Size, capRange.GetRequiredBytes())
		}
	}

	if topologyRequirement != nil && topologyRequirement.GetRequisite() != nil {
		reqTopology := topologyRequirement.GetRequisite()
		if len(reqTopology) > 1 {
			return false, "Too many topology requirements"
		}
		t, err := NewTopology(reqTopology[0])
		if err != nil {
			return false, "Cannot parse topology requirements"
		}
		if t.ZoneID != vol.ZoneID {
			return false, fmt.Sprintf("Volume in zone %s, requested zone is %s", vol.ZoneID, t.ZoneID)
		}
	}

	return true, ""
}

func determineSize(req *csi.CreateVolumeRequest) (int64, error) {
	var sizeInGB int64

	if req.GetCapacityRange() != nil {
		capRange := req.GetCapacityRange()

		required := capRange.GetRequiredBytes()
		sizeInGB = util.RoundUpBytesToGB(required)
		if sizeInGB == 0 {
			sizeInGB = 1
		}

		if limit := capRange.GetLimitBytes(); limit > 0 {
			if util.GigaBytesToBytes(sizeInGB) > limit {
				return 0, fmt.Errorf("after round-up, volume size %v GB exceeds the limit specified of %v bytes", sizeInGB, limit)
			}
		}
	}

	if sizeInGB == 0 {
		sizeInGB = 1
	}
	return sizeInGB, nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	volumeID := req.GetVolumeId()

	if acquired := cs.volumeLocks.TryAcquire(volumeID); !acquired {
		ctxzap.Extract(ctx).Sugar().Errorf(util.VolumeOperationAlreadyExistsFmt, volumeID)
		return nil, status.Errorf(codes.Aborted, util.VolumeOperationAlreadyExistsFmt, volumeID)
	}
	defer cs.volumeLocks.Release(volumeID)

	ctxzap.Extract(ctx).Sugar().Infow("Deleting volume",
		"volumeID", volumeID,
	)

	err := cs.connector.DeleteVolume(ctx, volumeID)
	if err != nil && err != cloud.ErrNotFound {
		return nil, status.Errorf(codes.Internal, "Cannot delete volume %s: %s", volumeID, err.Error())
	}
	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	// Check arguments

	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	volumeID := req.GetVolumeId()

	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Node ID missing in request")
	}
	nodeID := req.GetNodeId()

	if req.GetReadonly() {
		return nil, status.Error(codes.InvalidArgument, "Readonly not possible")
	}

	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}
	if req.GetVolumeCapability().AccessMode.Mode != onlyVolumeCapAccessMode.GetMode() {
		return nil, status.Error(codes.InvalidArgument, "Access mode not accepted")
	}

	ctxzap.Extract(ctx).Sugar().Infow("Initiating attaching volume",
		"volumeID", volumeID,
		"nodeID", nodeID,
	)

	// Check volume
	vol, err := cs.connector.GetVolumeByID(ctx, volumeID)
	if err == cloud.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "Volume %v not found", volumeID)
	} else if err != nil {
		// Error with CloudStack
		return nil, status.Errorf(codes.Internal, "Error %v", err)
	}

	if vol.VirtualMachineID != "" && vol.VirtualMachineID != nodeID {
		ctxzap.Extract(ctx).Sugar().Errorw("Volume already attached to another node",
			"volumeID", volumeID,
			"nodeID", nodeID,
			"attached nodeID", vol.VirtualMachineID,
		)
		return nil, status.Error(codes.AlreadyExists, "Volume already assigned to another node")
	}

	if _, err := cs.connector.GetVMByID(ctx, nodeID); err == cloud.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "VM %v not found", nodeID)
	} else if err != nil {
		// Error with CloudStack
		return nil, status.Errorf(codes.Internal, "Error %v", err)
	}

	if vol.VirtualMachineID == nodeID {
		// volume already attached
		ctxzap.Extract(ctx).Sugar().Infow("Volume already attached to node",
			"volumeID", volumeID,
			"nodeID", nodeID,
			"deviceID", vol.DeviceID,
		)
		publishContext := map[string]string{
			deviceIDContextKey: vol.DeviceID,
		}
		return &csi.ControllerPublishVolumeResponse{PublishContext: publishContext}, nil
	}

	ctxzap.Extract(ctx).Sugar().Infow("Attaching volume to node",
		"volumeID", volumeID,
		"nodeID", nodeID,
	)

	deviceID, err := cs.connector.AttachVolume(ctx, volumeID, nodeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Cannot attach volume %s: %s", volumeID, err.Error())
	}

	ctxzap.Extract(ctx).Sugar().Infow("Attached volume to node successfully",
		"volumeID", volumeID,
		"nodeID", nodeID,
	)

	publishContext := map[string]string{
		deviceIDContextKey: deviceID,
	}
	return &csi.ControllerPublishVolumeResponse{PublishContext: publishContext}, nil
}

func (cs *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	// Check arguments

	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	volumeID := req.GetVolumeId()
	nodeID := req.GetNodeId()

	// Check volume
	if vol, err := cs.connector.GetVolumeByID(ctx, volumeID); err == cloud.ErrNotFound {
		// Volume does not exist in CloudStack. We can safely assume this volume is no longer attached
		// The spec requires us to return OK here
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	} else if err != nil {
		// Error with CloudStack
		return nil, status.Errorf(codes.Internal, "Error %v", err)
	} else if nodeID != "" && vol.VirtualMachineID != nodeID {
		// Volume is present but not attached to this particular nodeID
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	// Check VM existence
	if _, err := cs.connector.GetVMByID(ctx, nodeID); err == cloud.ErrNotFound {
		// volumes cannot be attached to deleted VMs
		ctxzap.Extract(ctx).Sugar().Warnw("VM not found, marking ControllerUnpublishVolume successful",
			"volumeID", volumeID,
			"nodeID", nodeID,
		)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	} else if err != nil {
		// Error with CloudStack
		return nil, status.Errorf(codes.Internal, "Error %v", err)
	}

	ctxzap.Extract(ctx).Sugar().Infow("Detaching volume from node",
		"volumeID", volumeID,
		"nodeID", nodeID,
	)

	err := cs.connector.DetachVolume(ctx, volumeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Cannot detach volume %s: %s", volumeID, err.Error())
	}

	ctxzap.Extract(ctx).Sugar().Infow("Detached volume from node successfully",
		"volumeID", volumeID,
		"nodeID", nodeID,
	)

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	volCaps := req.GetVolumeCapabilities()
	if len(volCaps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities not provided")
	}

	if _, err := cs.connector.GetVolumeByID(ctx, volumeID); err == cloud.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "Volume %v not found", volumeID)
	} else if err != nil {
		// Error with CloudStack
		return nil, status.Errorf(codes.Internal, "Error %v", err)
	}

	if !isValidVolumeCapabilities(volCaps) {
		return &csi.ValidateVolumeCapabilitiesResponse{Message: "Requested VolumeCapabilities are invalid"}, nil
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.GetVolumeContext(),
			VolumeCapabilities: volCaps,
			Parameters:         req.GetParameters(),
		}}, nil
}

func isValidVolumeCapabilities(volCaps []*csi.VolumeCapability) bool {
	for _, c := range volCaps {
		if c.GetAccessMode() != nil && c.GetAccessMode().GetMode() != onlyVolumeCapAccessMode.GetMode() {
			return false
		}
	}
	return true
}

func (cs *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	expandVolumeLock := util.NewOperationLock(ctx)
	logger := ctxzap.Extract(ctx).Sugar()
	logger.Infow("Expand Volume: called with args", "args", protosanitizer.StripSecrets(*req))

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}
	err := expandVolumeLock.GetExpandLock(volumeID)
	if err != nil {

		logger.Errorf(util.VolumeOperationAlreadyExistsFmt, volumeID)
		return nil, status.Errorf(codes.Aborted, util.VolumeOperationAlreadyExistsFmt, volumeID)
	}
	defer expandVolumeLock.ReleaseExpandLock(volumeID)

	cap := req.GetCapacityRange()
	if cap == nil {
		return nil, status.Error(codes.InvalidArgument, "Capacity range not provided")
	}

	volSizeBytes := cap.GetRequiredBytes()
	volSizeGB := util.RoundUpBytesToGB(volSizeBytes)
	maxVolSize := cap.GetLimitBytes()

	if maxVolSize > 0 && maxVolSize < util.GigaBytesToBytes(volSizeGB) {
		return nil, status.Error(codes.OutOfRange, "Volume size exceeds the limit specified")
	}

	volume, err := cs.connector.GetVolumeByID(ctx, volumeID)
	if err != nil {
		if err == cloud.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "Volume %v not found", volumeID)
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("GetVolume failed with error %v", err))
	}

	if volume.Size >= util.GigaBytesToBytes(volSizeGB) {
		// A volume was already resized
		logger.Infof("Volume %q has been already expanded to %d. requested %d", volumeID, volume.Size, volSizeGB)

		return &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         volume.Size,
			NodeExpansionRequired: true,
		}, nil

	}
	err = cs.connector.ExpandVolume(ctx, volumeID, volSizeGB)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Could not resize volume %q to size %v: %v", volumeID, volSizeGB, err)
	}

	logger.Infow("ControllerExpandVolume resized",
		"requested_volume_ID", volumeID,
		"new_size", volSizeGB,
	)

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         util.GigaBytesToBytes(volSizeGB),
		NodeExpansionRequired: true,
	}, nil
}

func (cs *controllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
					},
				},
			},
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
					},
				},
			},
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
					},
				},
			},
		},
	}, nil
}

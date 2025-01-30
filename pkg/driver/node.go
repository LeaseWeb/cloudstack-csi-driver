package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/leaseweb/cloudstack-csi-driver/pkg/cloud"
	"github.com/leaseweb/cloudstack-csi-driver/pkg/mount"
	"github.com/leaseweb/cloudstack-csi-driver/pkg/util"
)

const (
	// default file system type to be used when it is not provided.
	defaultFsType = FSTypeExt4
)

var ValidFSTypes = map[string]struct{}{
	FSTypeExt2: {},
	FSTypeExt3: {},
	FSTypeExt4: {},
	FSTypeXfs:  {},
}

// NodeService represents the node service of CSI driver.
type NodeService struct {
	csi.UnimplementedNodeServer
	connector         cloud.Cloud
	mounter           mount.Mounter
	maxVolumesPerNode int64
	nodeName          string
	volumeLocks       *util.VolumeLocks
}

// NewNodeService creates a new node service.
func NewNodeService(connector cloud.Cloud, mounter mount.Mounter, options *Options) *NodeService {
	if mounter == nil {
		mounter = mount.New()
	}

	return &NodeService{
		connector:         connector,
		mounter:           mounter,
		maxVolumesPerNode: options.VolumeAttachLimit,
		nodeName:          options.NodeName,
		volumeLocks:       util.NewVolumeLocks(),
	}
}

func (ns *NodeService) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("NodeStageVolume: called", "args", req)

	// Check parameters

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	target := req.GetStagingTargetPath()
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "Staging target not provided")
	}

	volCap := req.GetVolumeCapability()
	if volCap == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not provided")
	}
	if !isValidVolumeCapabilities([]*csi.VolumeCapability{volCap}) {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not supported")
	}

	// If the access type is block, do nothing for stage
	if blk := volCap.GetBlock(); blk != nil {
		return &csi.NodeStageVolumeResponse{}, nil
	}

	mnt := volCap.GetMount()
	if mnt == nil {
		return nil, status.Error(codes.InvalidArgument, "NodeStageVolume: mount volume capability not found")
	}

	fsType := mnt.GetFsType()
	if fsType == "" {
		fsType = defaultFsType
	}

	_, ok := ValidFSTypes[strings.ToLower(fsType)]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "NodeStageVolume: invalid fstype %s", fsType)
	}

	var mountOptions []string
	for _, f := range mnt.GetMountFlags() {
		if !hasMountOption(mountOptions, f) {
			mountOptions = append(mountOptions, f)
		}
	}

	if acquired := ns.volumeLocks.TryAcquire(volumeID); !acquired {
		return nil, status.Errorf(codes.Aborted, util.VolumeOperationAlreadyExistsVolumeIDFmt, volumeID)
	}
	defer func() {
		logger.V(4).Info("NodeStageVolume: volume operation finished", "volumeId", volumeID)
		ns.volumeLocks.Release(volumeID)
	}()

	// Now, find the device path
	source, err := ns.mounter.GetDevicePath(ctx, volumeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Cannot find device path for volume %s: %s", volumeID, err.Error())
	}

	logger.V(4).Info("NodeStageVolume: device found",
		"source", source,
	)

	exists, err := ns.mounter.PathExists(target)
	if err != nil {
		msg := fmt.Sprintf("failed to check if target %q exists: %v", target, err)

		return nil, status.Error(codes.Internal, msg)
	}
	if !exists {
		// If target path does not exist we need to create the directory where volume will be staged
		logger.V(4).Info("NodeStageVolume: creating target dir", "target", target)
		if err = ns.mounter.MakeDir(target); err != nil {
			msg := fmt.Sprintf("could not create target dir %q: %v", target, err)

			return nil, status.Error(codes.Internal, msg)
		}
	}

	// Check if a device is mounted in target directory
	device, _, err := ns.mounter.GetDeviceNameFromMount(target)
	if err != nil {
		msg := fmt.Sprintf("failed to check if volume is already mounted: %v", err)

		return nil, status.Error(codes.Internal, msg)
	}

	// This operation (NodeStageVolume) MUST be idempotent.
	// If the volume corresponding to the volume_id is already staged to the staging_target_path,
	// and is identical to the specified volume_capability the Plugin MUST reply 0 OK.
	logger.V(4).Info("NodeStageVolume: checking if volume is already staged", "device", device, "source", source, "target", target)
	if device == source {
		logger.V(4).Info("NodeStageVolume: volume already staged", "volumeID", volumeID)

		return &csi.NodeStageVolumeResponse{}, nil
	}

	logger.V(4).Info("NodeStageVolume: staging volume", "source", source, "volumeID", volumeID, "target", target, "fstype", fsType, "options", mountOptions)
	err = ns.mounter.FormatAndMount(source, target, fsType, mountOptions)
	if err != nil {
		msg := fmt.Sprintf("could not format %q and mount it at %q: %v", source, target, err)

		return nil, status.Error(codes.Internal, msg)
	}

	needResize, err := ns.mounter.NeedResize(source, target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Could not determine if volume %q (%q) needs to be resized:  %v", volumeID, source, err)
	}

	if needResize {
		logger.V(2).Info("NodeStageVolume: volume needs resizing", "source", source)
		if _, err := ns.mounter.Resize(source, target); err != nil {
			return nil, status.Errorf(codes.Internal, "could not resize volume %q (%q):  %v", volumeID, source, err)
		}
	}
	logger.V(4).Info("NodeStageVolume: successfully staged volume", "source", source, "volumeID", volumeID, "target", target, "fstype", fsType)

	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *NodeService) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("NodeUnstageVolume: called", "args", req)

	// Check parameters
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	target := req.GetStagingTargetPath()
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "Staging target not provided")
	}

	if acquired := ns.volumeLocks.TryAcquire(volumeID); !acquired {
		return nil, status.Errorf(codes.Aborted, util.VolumeOperationAlreadyExistsVolumeIDFmt, volumeID)
	}
	defer func() {
		logger.V(4).Info("NodeUnstageVolume: volume operation finished", "volumeId", volumeID)
		ns.volumeLocks.Release(volumeID)
	}()

	// Check if target directory is a mount point. GetDeviceNameFromMount
	// given a mnt point, finds the device from /proc/mounts
	// returns the device name, reference count, and error code
	dev, refCount, err := ns.mounter.GetDeviceNameFromMount(target)
	if err != nil {
		msg := fmt.Sprintf("failed to check if target %q is a mount point: %v", target, err)

		return nil, status.Error(codes.Internal, msg)
	}

	// From the spec: If the volume corresponding to the volume_id
	// is not staged to the staging_target_path, the Plugin MUST
	// reply 0 OK.
	if refCount == 0 {
		logger.V(4).Info("NodeUnstageVolume: target not mounted", "target", target)

		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	if refCount > 1 {
		logger.V(4).Info("NodeUnstageVolume: found references to device mounted at target path", "refCount", refCount, "device", dev, "target", target)
	}

	logger.V(4).Info("NodeUnstageVolume: unmounting", "target", target)

	err = ns.mounter.Unstage(target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmount target %q: %v", target, err)
	}

	logger.V(4).Info("NodeUnstageVolume: unmount successful",
		"target", target,
		"volumeID", volumeID,
	)

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *NodeService) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("NodePublishVolume: called", "args", req)

	// Check arguments
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	source := req.GetStagingTargetPath()
	if source == "" {
		return nil, status.Error(codes.InvalidArgument, "Staging target path missing in request")
	}

	target := req.GetTargetPath()
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	volCap := req.GetVolumeCapability()
	if volCap == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}

	if !isValidVolumeCapabilities([]*csi.VolumeCapability{volCap}) {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not supported")
	}

	if acquired := ns.volumeLocks.TryAcquire(target); !acquired {
		return nil, status.Errorf(codes.Aborted, util.TargetPathOperationAlreadyExistsFmt, target)
	}
	defer func() {
		logger.V(4).Info("NodePublishVolume: volume operation finished", "volumeId", volumeID)
		ns.volumeLocks.Release(target)
	}()

	mountOptions := []string{"bind"}
	if req.GetReadonly() {
		mountOptions = append(mountOptions, "ro")
	}

	switch mode := req.GetVolumeCapability().GetAccessType().(type) {
	case *csi.VolumeCapability_Mount:
		if err := ns.nodePublishVolumeForMount(ctx, req, mountOptions, mode); err != nil {
			return nil, err
		}
	case *csi.VolumeCapability_Block:
		if err := ns.nodePublishVolumeForBlock(ctx, req, mountOptions); err != nil {
			return nil, err
		}
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *NodeService) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("NodeUnpublishVolume: called", "args", req)

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	target := req.GetTargetPath()
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	if acquired := ns.volumeLocks.TryAcquire(target); !acquired {
		return nil, status.Errorf(codes.Aborted, util.TargetPathOperationAlreadyExistsFmt, target)
	}
	defer func() {
		logger.V(4).Info("NodeUnpublishVolume: volume operation finished", "volumeId", volumeID)
		ns.volumeLocks.Release(target)
	}()

	logger.V(4).Info("NodeUnpublishVolume: unmounting volume",
		"target", target,
		"volumeID", volumeID,
	)

	err := ns.mounter.Unpublish(target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmount target %q: %v", target, err)
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *NodeService) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("NodeGetInfo: called", "args", req)

	if ns.nodeName == "" {
		return nil, status.Error(codes.Internal, "Missing node name")
	}

	vm, err := ns.connector.GetNodeInfo(ctx, ns.nodeName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if vm.ID == "" {
		return nil, status.Error(codes.Internal, "Node with no ID")
	}
	if vm.ZoneID == "" {
		return nil, status.Error(codes.Internal, "Node zone ID not found")
	}

	topology := Topology{ZoneID: vm.ZoneID}

	return &csi.NodeGetInfoResponse{
		NodeId:             vm.ID,
		AccessibleTopology: topology.ToCSI(),
		MaxVolumesPerNode:  ns.maxVolumesPerNode,
	}, nil
}

func (ns *NodeService) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("NodeExpandVolume: called", "args", req)

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	// Get volume path
	// This should work for Kubernetes >= 1.26, see https://github.com/kubernetes/kubernetes/issues/115343
	volumePath := req.GetStagingTargetPath()
	if volumePath == "" {
		// Except that it doesn't work in the sanity test, so we need a fallback to volumePath.
		volumePath = req.GetVolumePath()
	}
	if len(volumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume path not provided")
	}

	volCap := req.GetVolumeCapability()
	if volCap != nil { //nolint:nestif
		caps := []*csi.VolumeCapability{volCap}
		if !isValidVolumeCapabilities(caps) {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("VolumeCapability is invalid: %v", volCap))
		}

		switch volCap.GetAccessType().(type) { //nolint:gocritic
		case *csi.VolumeCapability_Block:
			logger.Info("Filesystem expansion is skipped for block volumes")

			return &csi.NodeExpandVolumeResponse{}, nil
		}
	} else {
		// VolumeCapability is nil, check if volumePath point to a block device
		isBlock, err := ns.mounter.IsBlockDevice(volumePath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to determine if volumePath [%v] is a block device: %v", volumePath, err)
		}
		if isBlock {
			// Skip resizing for Block NodeExpandVolume
			bcap, err := ns.mounter.GetBlockSizeBytes(volumePath)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to get block capacity on path %s: %v", req.GetVolumePath(), err)
			}
			logger.V(4).Info("NodeExpandVolume: called, since given volumePath is a block device, ignoring...", "volumeID", volumeID, "volumePath", volumePath)

			return &csi.NodeExpandVolumeResponse{CapacityBytes: bcap}, nil
		}
	}

	if acquired := ns.volumeLocks.TryAcquire(volumeID); !acquired {
		return nil, status.Errorf(codes.Aborted, util.VolumeOperationAlreadyExistsVolumeIDFmt, volumeID)
	}
	defer func() {
		logger.V(4).Info("NodeExpandVolume: volume operation finished", "volumeId", volumeID)
		ns.volumeLocks.Release(volumeID)
	}()

	_, err := ns.connector.GetVolumeByID(ctx, volumeID)
	if err != nil {
		if errors.Is(err, cloud.ErrNotFound) {
			return nil, status.Error(codes.NotFound, fmt.Sprintf("Volume with ID %s not found", volumeID))
		}

		return nil, status.Error(codes.Internal, fmt.Sprintf("NodeExpandVolume failed with error %v", err))
	}

	devicePath, err := ns.mounter.GetDevicePath(ctx, volumeID)
	if devicePath == "" {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Unable to find Device path for volume %s: %v", volumeID, err))
	}

	logger.Info("Expanding volume",
		"devicePath", devicePath,
		"volumeID", volumeID,
		"volumePath", volumePath,
	)

	if _, err := ns.mounter.Resize(devicePath, volumePath); err != nil {
		return nil, status.Errorf(codes.Internal, "Could not resize volume %q (%q): %v", volumeID, devicePath, err)
	}

	bcap, err := ns.mounter.GetBlockSizeBytes(devicePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get block capacity on path %s: %v", req.GetVolumePath(), err)
	}

	return &csi.NodeExpandVolumeResponse{CapacityBytes: bcap}, nil
}

func (ns *NodeService) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("NodeGetVolumeStats: called", "args", req)

	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	if req.GetVolumePath() == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume Path missing in request")
	}

	exists, err := ns.mounter.PathExists(req.GetVolumePath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unknown error when stat on %s: %v", req.GetVolumePath(), err)
	}
	if !exists {
		return nil, status.Errorf(codes.NotFound, "path %s does not exist", req.GetVolumePath())
	}

	isBlock, err := ns.mounter.IsBlockDevice(req.GetVolumePath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to determine if %q is block device: %s", req.GetVolumePath(), err)
	}

	if isBlock {
		bcap, blockErr := ns.mounter.GetBlockSizeBytes(req.GetVolumePath())
		if blockErr != nil {
			return nil, status.Errorf(codes.Internal, "failed to get block capacity on path %s: %v", req.GetVolumePath(), blockErr)
		}

		return &csi.NodeGetVolumeStatsResponse{
			Usage: []*csi.VolumeUsage{
				{
					Unit:  csi.VolumeUsage_BYTES,
					Total: bcap,
				},
			},
		}, nil
	}

	stats, err := ns.mounter.GetStatistics(req.GetVolumePath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to retrieve capacity statistics for volume path %q: %s", req.GetVolumePath(), err)
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Available: stats.AvailableBytes,
				Total:     stats.TotalBytes,
				Used:      stats.UsedBytes,
				Unit:      csi.VolumeUsage_BYTES,
			},
			{
				Available: stats.AvailableInodes,
				Total:     stats.TotalInodes,
				Used:      stats.UsedInodes,
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
}

func (ns *NodeService) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	resp := &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
					},
				},
			},
		},
	}

	return resp, nil
}

func (ns *NodeService) nodePublishVolumeForBlock(ctx context.Context, req *csi.NodePublishVolumeRequest, mountOptions []string) error {
	logger := klog.FromContext(ctx)
	target := req.GetTargetPath()
	volumeID := req.GetVolumeId()
	source, err := ns.mounter.GetDevicePath(ctx, volumeID)
	if err != nil {
		return status.Errorf(codes.Internal, "Cannot find device path for volume %s: %v", volumeID, err)
	}

	globalMountPath := filepath.Dir(target)
	exists, err := ns.mounter.PathExists(globalMountPath)
	if err != nil {
		return status.Errorf(codes.Internal, "Could not check if path exists %q: %v", globalMountPath, err)
	}
	if !exists {
		if err = ns.mounter.MakeDir(globalMountPath); err != nil {
			return status.Errorf(codes.Internal, "Could not create dir %q: %v", globalMountPath, err)
		}
	}

	// Create the mount point as a file since bind mount device node requires it to be a file
	logger.V(4).Info("NodePublishVolume: making target file", "target", target)
	err = ns.mounter.MakeFile(target)
	if err != nil {
		if removeErr := os.Remove(target); removeErr != nil {
			return status.Errorf(codes.Internal, "Could not remove mount target %q: %v", target, removeErr)
		}

		return status.Errorf(codes.Internal, "Could not create file %q: %v", target, err)
	}

	mounted, err := ns.isMounted(ctx, target)
	if err != nil {
		return status.Errorf(codes.Internal, "Could not check if %q is mounted: %v", target, err)
	}

	if !mounted {
		logger.V(4).Info("NodePublishVolume: mounting block device",
			"source", source,
			"target", target,
		)

		if err := ns.mounter.Mount(source, target, "", mountOptions); err != nil {
			if removeErr := os.Remove(target); removeErr != nil {
				return status.Errorf(codes.Internal, "Could not remove mount target %q: %v", target, removeErr)
			}

			return status.Errorf(codes.Internal, "failed to mount %q at %q: %v", source, target, err)
		}
	} else {
		logger.V(4).Info("NodePublishVolume: target path already mounted", "target", target)
	}

	return nil
}

func (ns *NodeService) nodePublishVolumeForMount(ctx context.Context, req *csi.NodePublishVolumeRequest, mountOptions []string, mode *csi.VolumeCapability_Mount) error {
	logger := klog.FromContext(ctx)
	target := req.GetTargetPath()
	source := req.GetStagingTargetPath()

	if m := mode.Mount; m != nil {
		for _, f := range m.GetMountFlags() {
			if !hasMountOption(mountOptions, f) {
				mountOptions = append(mountOptions, f)
			}
		}
	}

	// Prepare the publish target
	logger.V(4).Info("NodePublishVolume: creating dir", "target", target)
	if err := ns.mounter.MakeDir(target); err != nil {
		return status.Errorf(codes.Internal, "Could not create dir %q: %v", target, err)
	}

	mounted, err := ns.isMounted(ctx, target)
	if err != nil {
		return status.Errorf(codes.Internal, "Could not check if %q is mounted: %v", target, err)
	}

	if !mounted {
		fsType := mode.Mount.GetFsType()
		if fsType == "" {
			fsType = defaultFsType
		}

		_, ok := ValidFSTypes[strings.ToLower(fsType)]
		if !ok {
			return status.Errorf(codes.InvalidArgument, "NodePublishVolume: invalid fstype %s", fsType)
		}

		mountOptions = collectMountOptions(fsType, mountOptions)

		logger.V(4).Info("NodePublishVolume: mounting source",
			"source", source,
			"target", target,
			"fsType", fsType,
			"mountOptions", mountOptions,
		)

		if err := ns.mounter.Mount(source, target, fsType, mountOptions); err != nil {
			return status.Errorf(codes.Internal, "Failed to mount %q at %q: %v", source, target, err)
		}
	}

	return nil
}

func (ns *NodeService) isMounted(ctx context.Context, target string) (bool, error) {
	logger := klog.FromContext(ctx)

	notMnt, err := ns.mounter.IsLikelyNotMountPoint(target)
	if err != nil && !os.IsNotExist(err) {
		// Checking if the path exists and error is related to Corrupted Mount, in that case, the system could unmount and mount.
		_, pathErr := ns.mounter.PathExists(target)
		if pathErr != nil && ns.mounter.IsCorruptedMnt(pathErr) {
			logger.V(4).Info("NodePublishVolume: Target path is a corrupted mount. Trying to unmount.", "target", target)
			if mntErr := ns.mounter.Unpublish(target); mntErr != nil {
				return false, fmt.Errorf("unable to unmount the target %q : %w", target, mntErr)
			}

			// After successful unmount, the device is ready to be mounted.
			return false, nil
		}

		return false, fmt.Errorf("could not check if %q is a mount point: %w, %w", target, err, pathErr)
	}

	// Do not return os.IsNotExist error. Other errors were handled above. The
	// existence of the target should be checked by the caller explicitly and
	// independently because sometimes prior to mount it is expected not to exist.
	if err != nil && os.IsNotExist(err) {
		logger.V(5).Info("[Debug] NodePublishVolume: Target path does not exist", "target", target)

		return false, nil
	}

	if !notMnt {
		logger.V(4).Info("NodePublishVolume: Target path is already mounted", "target", target)
	}

	return !notMnt, nil
}

// hasMountOption returns a boolean indicating whether the given
// slice already contains a mount option. This is used to prevent
// passing duplicate option to the mount command.
func hasMountOption(options []string, opt string) bool {
	for _, o := range options {
		if o == opt {
			return true
		}
	}

	return false
}

// collectMountOptions returns array of mount options from
// VolumeCapability_MountVolume and special mount options for
// given filesystem.
func collectMountOptions(fsType string, mntFlags []string) []string {
	var options []string
	for _, opt := range mntFlags {
		if !hasMountOption(options, opt) {
			options = append(options, opt)
		}
	}

	// By default, xfs does not allow mounting of two volumes with the same filesystem uuid.
	// Force ignore this uuid to be able to mount volume + its clone / restored snapshot on the same node.
	if fsType == FSTypeXfs {
		if !hasMountOption(options, "nouuid") {
			options = append(options, "nouuid")
		}
	}

	return options
}

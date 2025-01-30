// Package mount provides utilities to detect,
// format and mount storage devices.
package mount

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	kexec "k8s.io/utils/exec"
)

const (
	diskIDPath = "/dev/disk/by-id"
)

// Mounter defines the set of methods to allow for
// mount operations on a system.
type Mounter interface { //nolint:interfacebloat
	mount.Interface

	FormatAndMount(source string, target string, fstype string, options []string) error
	GetBlockSizeBytes(devicePath string) (int64, error)
	GetDevicePath(ctx context.Context, volumeID string) (string, error)
	GetDeviceNameFromMount(mountPath string) (string, int, error)
	GetStatistics(volumePath string) (VolumeStatistics, error)
	IsBlockDevice(devicePath string) (bool, error)
	IsCorruptedMnt(err error) bool
	MakeDir(path string) error
	MakeFile(path string) error
	NeedResize(devicePath string, deviceMountPath string) (bool, error)
	PathExists(path string) (bool, error)
	Resize(devicePath, deviceMountPath string) (bool, error)
	Unpublish(path string) error
	Unstage(path string) error
}

// NodeMounter implements Mounter.
// A superstruct of SafeFormatAndMount.
type NodeMounter struct {
	*mount.SafeFormatAndMount
}

type VolumeStatistics struct {
	AvailableBytes, TotalBytes, UsedBytes    int64
	AvailableInodes, TotalInodes, UsedInodes int64
}

// New creates an implementation of the mount.Mounter.
func New() Mounter {
	return &NodeMounter{
		&mount.SafeFormatAndMount{
			Interface: mount.New(""),
			Exec:      kexec.New(),
		},
	}
}

// GetBlockSizeBytes gets the size of the disk in bytes.
func (m *NodeMounter) GetBlockSizeBytes(devicePath string) (int64, error) {
	output, err := m.Exec.Command("blockdev", "--getsize64", devicePath).Output()
	if err != nil {
		return -1, fmt.Errorf("error when getting size of block volume at path %s: output: %s, err: %w", devicePath, string(output), err)
	}
	strOut := strings.TrimSpace(string(output))
	gotSizeBytes, err := strconv.ParseInt(strOut, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("failed to parse size %s as int", strOut)
	}

	return gotSizeBytes, nil
}

func (m *NodeMounter) GetDevicePath(ctx context.Context, volumeID string) (string, error) {
	backoff := wait.Backoff{
		Duration: 1 * time.Second,
		Factor:   1.1,
		Steps:    15,
	}

	var devicePath string
	err := wait.ExponentialBackoffWithContext(ctx, backoff, func(context.Context) (bool, error) {
		path, err := m.getDevicePathBySerialID(volumeID)
		if err != nil {
			return false, err
		}
		if path != "" {
			devicePath = path

			return true, nil
		}
		m.probeVolume(ctx)

		return false, nil
	})

	if wait.Interrupted(err) {
		return "", fmt.Errorf("failed to find device for the volumeID: %q within the alloted time", volumeID)
	} else if devicePath == "" {
		return "", fmt.Errorf("device path was empty for volumeID: %q", volumeID)
	}

	return devicePath, nil
}

func (m *NodeMounter) getDevicePathBySerialID(volumeID string) (string, error) {
	sourcePathPrefixes := []string{"virtio-", "scsi-", "scsi-0QEMU_QEMU_HARDDISK_"}
	serial := diskUUIDToSerial(volumeID)
	for _, prefix := range sourcePathPrefixes {
		source := filepath.Join(diskIDPath, prefix+serial)
		_, err := os.Stat(source)
		if err == nil {
			return source, nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
	}

	return "", nil
}

func (m *NodeMounter) probeVolume(ctx context.Context) {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Scanning SCSI host")

	scsiPath := "/sys/class/scsi_host/"
	if dirs, err := os.ReadDir(scsiPath); err == nil {
		for _, f := range dirs {
			name := scsiPath + f.Name() + "/scan"
			data := []byte("- - -")
			logger.V(2).Info("Triggering SCSI host rescan")
			if err = os.WriteFile(name, data, 0o666); err != nil { //nolint:gosec
				logger.Error(err, "Failed to rescan scsi host ", "dirName", name)
			}
		}
	} else {
		logger.Error(err, "Failed to read dir ", "dirName", scsiPath)
	}

	args := []string{"trigger"}
	cmd := m.Exec.Command("udevadm", args...)
	_, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error(err, "Error running udevadm trigger")
	}
}

func (m *NodeMounter) GetDeviceNameFromMount(mountPath string) (string, int, error) {
	return mount.GetDeviceNameFromMount(m, mountPath)
}

// diskUUIDToSerial reproduces CloudStack function diskUuidToSerial
// from https://github.com/apache/cloudstack/blob/0f3f2a0937/plugins/hypervisors/kvm/src/main/java/com/cloud/hypervisor/kvm/resource/LibvirtComputingResource.java#L3000
//
// This is what CloudStack do *with KVM hypervisor* to translate
// a CloudStack volume UUID to libvirt disk serial.
func diskUUIDToSerial(uuid string) string {
	uuidWithoutHyphen := strings.ReplaceAll(uuid, "-", "")
	if len(uuidWithoutHyphen) < 20 {
		return uuidWithoutHyphen
	}

	return uuidWithoutHyphen[:20]
}

func (*NodeMounter) PathExists(path string) (bool, error) {
	return mount.PathExists(path)
}

func (*NodeMounter) MakeDir(path string) error {
	err := os.MkdirAll(path, os.FileMode(0o755))
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}

	return nil
}

func (*NodeMounter) MakeFile(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE, os.FileMode(0o644))
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	if err = f.Close(); err != nil {
		return err
	}

	return nil
}

// Resize resizes the filesystem of the given devicePath.
func (m *NodeMounter) Resize(devicePath, deviceMountPath string) (bool, error) {
	return mount.NewResizeFs(m.Exec).Resize(devicePath, deviceMountPath)
}

// NeedResize checks if the filesystem of the given devicePath needs to be resized.
func (m *NodeMounter) NeedResize(devicePath string, deviceMountPath string) (bool, error) {
	return mount.NewResizeFs(m.Exec).NeedResize(devicePath, deviceMountPath)
}

// GetStatistics gathers statistics on the volume.
func (m *NodeMounter) GetStatistics(volumePath string) (VolumeStatistics, error) {
	isBlock, err := m.IsBlockDevice(volumePath)
	if err != nil {
		return VolumeStatistics{}, fmt.Errorf("failed to determine if volume %s is block device: %w", volumePath, err)
	}

	if isBlock {
		// See http://man7.org/linux/man-pages/man8/blockdev.8.html for details
		output, err := exec.Command("blockdev", "getsize64", volumePath).CombinedOutput()
		if err != nil {
			return VolumeStatistics{}, fmt.Errorf("error when getting size of block volume at path %s: output: %s, err: %w", volumePath, string(output), err)
		}
		strOut := strings.TrimSpace(string(output))
		gotSizeBytes, err := strconv.ParseInt(strOut, 10, 64)
		if err != nil {
			return VolumeStatistics{}, fmt.Errorf("failed to parse size %s into int", strOut)
		}

		return VolumeStatistics{
			TotalBytes: gotSizeBytes,
		}, nil
	}

	var statfs unix.Statfs_t
	// See http://man7.org/linux/man-pages/man2/statfs.2.html for details.
	err = unix.Statfs(volumePath, &statfs)
	if err != nil {
		return VolumeStatistics{}, err
	}

	volStats := VolumeStatistics{
		AvailableBytes: int64(statfs.Bavail) * int64(statfs.Bsize),                         //nolint:unconvert,gosec,nolintlint
		TotalBytes:     int64(statfs.Blocks) * int64(statfs.Bsize),                         //nolint:unconvert,gosec,nolintlint
		UsedBytes:      (int64(statfs.Blocks) - int64(statfs.Bfree)) * int64(statfs.Bsize), //nolint:unconvert,gosec,nolintlint

		AvailableInodes: int64(statfs.Ffree),                       //nolint:gosec
		TotalInodes:     int64(statfs.Files),                       //nolint:gosec
		UsedInodes:      int64(statfs.Files) - int64(statfs.Ffree), //nolint:gosec
	}

	return volStats, nil
}

// IsBlockDevice checks if the given path is a block device.
func (m *NodeMounter) IsBlockDevice(devicePath string) (bool, error) {
	var stat unix.Stat_t
	err := unix.Stat(devicePath, &stat)
	if err != nil {
		return false, err
	}

	return (stat.Mode & unix.S_IFMT) == unix.S_IFBLK, nil
}

// IsCorruptedMnt return true if err is about corrupted mount point.
func (m *NodeMounter) IsCorruptedMnt(err error) bool {
	return mount.IsCorruptedMnt(err)
}

// Unpublish unmounts the given path.
func (m *NodeMounter) Unpublish(path string) error {
	return m.Unstage(path)
}

// Unstage unmounts the given path.
func (m *NodeMounter) Unstage(path string) error {
	return mount.CleanupMountPoint(path, m, true)
}

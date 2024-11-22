package driver

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cloud "github.com/leaseweb/cloudstack-csi-driver/pkg/cloud/fake"
	"github.com/leaseweb/cloudstack-csi-driver/pkg/mount"
	"github.com/leaseweb/cloudstack-csi-driver/pkg/util"
)

const (
	sourceTest = "./source_test"
	targetTest = "./target_test"
)

func TestNodePublishVolumeIdempotentMount(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("Test requires root")
	}
	driver := &NodeService{
		connector:   cloud.New(),
		mounter:     mount.New(),
		volumeLocks: util.NewVolumeLocks(),
	}

	_ = driver.mounter.MakeDir(sourceTest)
	_ = driver.mounter.MakeDir(sourceTest)

	volumeCap := csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}
	req := csi.NodePublishVolumeRequest{
		VolumeCapability:  &csi.VolumeCapability{AccessMode: &volumeCap},
		VolumeId:          "vol_1",
		TargetPath:        targetTest,
		StagingTargetPath: sourceTest,
		Readonly:          true,
	}

	_, err := driver.NodePublishVolume(context.Background(), &req)
	require.NoError(t, err)
	_, err = driver.NodePublishVolume(context.Background(), &req)
	require.NoError(t, err)

	// ensure the target not be mounted twice
	targetAbs, err := filepath.Abs(targetTest)
	require.NoError(t, err)

	mountList, err := driver.mounter.List()
	require.NoError(t, err)
	mountPointNum := 0
	for _, mountPoint := range mountList {
		if mountPoint.Path == targetAbs {
			mountPointNum++
		}
	}
	assert.Equal(t, 1, mountPointNum)
	err = driver.mounter.Unmount(targetTest)
	require.NoError(t, err)
	_ = driver.mounter.Unmount(targetTest)
	err = os.RemoveAll(sourceTest)
	require.NoError(t, err)
	err = os.RemoveAll(targetTest)
	require.NoError(t, err)
}

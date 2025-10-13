package lsm

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBaseManifest  manifest 文件整体性测试
func TestBaseManifest(t *testing.T) {
	clearDir()
	recovery := func() {
		// 每次运行都是相当于意外重启
		lsm := buildLSM()
		// 测试正确性
		baseTest(t, lsm, 128)
		lsm.Close()
	}
	// 运行这个闭包5次进行测试
	runTest(5, recovery)
}

func TestManifestMagic(t *testing.T) {
	helpTestManifestFileCorruption(t, 3, "bad magic")
}

func TestManifestVersion(t *testing.T) {
	helpTestManifestFileCorruption(t, 4, "")
}

func TestManifestChecksum(t *testing.T) {
	helpTestManifestFileCorruption(t, 15, "")
}

func helpTestManifestFileCorruption(t *testing.T, off int64, errorContent string) {
	clearDir()
	// 创建lsm，然后再将其关闭
	{
		lsm := buildLSM()
		require.NoError(t, lsm.Close())
	}
	currentData, err := os.ReadFile(filepath.Join(opt.WorkDir, "CURRENT"))
	require.NoError(t, err)
	manifestName := strings.TrimSpace(string(currentData))
	fp, err := os.OpenFile(filepath.Join(opt.WorkDir, manifestName), os.O_RDWR, 0)
	require.NoError(t, err)
	// 写入一个错误的值
	_, err = fp.WriteAt([]byte{'X'}, off)
	require.NoError(t, err)
	require.NoError(t, fp.Close())
	defer func() {
		if err := recover(); err != nil && errorContent != "" {
			require.Contains(t, err.(error).Error(), errorContent)
		}
	}()
	// 在此打开 lsm 此时会panic
	lsm := buildLSM()
	require.NoError(t, lsm.Close())
}

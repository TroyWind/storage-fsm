package util

import (
	"crypto/rand"
	"encoding/binary"
	rand2 "math/rand"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"time"
)

func HomeDir() string {
	home := os.Getenv("HOME")
	if home == "" {
		if curU, err := user.Current(); err == nil {
			home = curU.HomeDir
		}
	}
	if home == "" {
		panic("can't get home dir")
	}
	return home
}

func FileExist(filePath string) bool {
	_, err := os.Stat(filePath)
	if err != nil && os.IsNotExist(err) {
		return false
	}

	return true
}

// 获取当前执行路径，包含执行文件
func CurExecPath() string {
	file, _ := exec.LookPath(os.Args[0])

	//得到全路径，比如在windows下E:\\golang\\test\\a.exe
	path, _ := filepath.Abs(file)

	//rst := filepath.Dir(path)

	return path
}

// 获取当前执行目录
func CurExecDir() string {
	return filepath.Dir(CurExecPath())
}

func RandANum(max int) int {
	x := make([]byte, 8)
	_, err := rand.Read(x)
	if err != nil {
		//log.L.Warn("crypto rand failed, use time seed rand", zap.Error(err))
		rand2.Seed(time.Now().Unix())
		return rand2.Intn(max)
	}
	return int(binary.BigEndian.Uint64(x) % uint64(max))
}

// 从请求中获取远端IP
func RemoteIPFromReqAddr(remoteAddr string) string {
	i := strings.Index(remoteAddr, ":")
	return remoteAddr[:i]
}

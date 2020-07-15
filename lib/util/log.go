package util

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

func GetXDebugLog(moduleName string) *zap.Logger {
	if os.Getenv("XLotusLogOn") != "" {
		l, err := LogToWorkDir(CurExecDir(), moduleName, zap.DebugLevel, true).Build()
		if err != nil {
			panic(err)
		}
		return l
	}

	l, err := LogNothing().Build()
	if err != nil {
		panic(err)
	}
	return l
}

func LogToWorkDir(workDir, moduleName string, level zapcore.Level, delOld bool) zap.Config {
	logP := filepath.Join(workDir, "logs")
	if !FileExist(logP) {
		if err := os.MkdirAll(logP, 0755); err != nil {
			panic(err)
		}
	}

	appName := filepath.Base(CurExecPath())
	outP := filepath.Join(logP, strings.Join([]string{moduleName, appName, "out.log"}, "-"))
	errP := filepath.Join(logP, strings.Join([]string{moduleName, appName, "err.log"}, "-"))
	if delOld {
		_ = os.Remove(outP)
		_ = os.Remove(errP)
	}
	if runtime.GOOS == "windows" {
		// 解决windows不支持sink
		_ = zap.RegisterSink("winfile", newWinFileSink)
		outP = "winfile:///" + outP
		errP = "winfile:///" + errP
	}
	//fmt.Println("-2-", filepath.Join(logP, "out.log"))

	cfg := zap.NewProductionConfig()
	cfg.Encoding = "console"
	cfg.OutputPaths = []string{outP}
	cfg.ErrorOutputPaths = []string{errP}
	cfg.Level = zap.NewAtomicLevelAt(level)
	cfg.EncoderConfig.TimeKey = "t"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	return cfg
}

func newWinFileSink(u *url.URL) (zap.Sink, error) {
	// Remove leading slash left by url.Parse()
	//fmt.Println("-1-", u, u.Path)
	return os.OpenFile(u.Path[1:], os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
}

func LogNothing() zap.Config {
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{}
	//cfg.ErrorOutputPaths = []string{}
	cfg.Level = zap.NewAtomicLevelAt(zap.FatalLevel)
	cfg.EncoderConfig.TimeKey = "t"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	return cfg
}

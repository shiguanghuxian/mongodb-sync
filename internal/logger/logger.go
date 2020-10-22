package logger

import (
	"log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	LogFileName      = "./logs/access.log"
	SplitFileMaxSize = 100
	DefaultLevel     = zapcore.InfoLevel
)

var (
	GlobalLogger *Logger
)

type Logger struct {
	*zap.SugaredLogger
	syncFile *lumberjack.Logger
}

// NewLogger 创建日志对象
func NewLogger(debug bool) *Logger {
	syncFile := &lumberjack.Logger{
		Filename:  LogFileName,
		MaxSize:   SplitFileMaxSize,
		LocalTime: true,
		Compress:  true,
	}
	syncWriter := zapcore.AddSync(syncFile)
	level := DefaultLevel
	if debug {
		level = zapcore.DebugLevel
	}
	encoder := zap.NewProductionEncoderConfig()
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoder), syncWriter, zap.NewAtomicLevelAt(zapcore.Level(level)))
	logger := zap.New(core, zap.AddCaller())
	sugaredLogger := logger.Sugar()
	GlobalLogger = &Logger{
		SugaredLogger: sugaredLogger,
		syncFile:      syncFile,
	}
	return GlobalLogger
}

// DestroyLogger 销毁日志
func DestroyLogger() {
	err := GlobalLogger.Sync()
	if err != nil {
		log.Println("刷新日志到磁盘错误", err)
	}
	err = GlobalLogger.syncFile.Close()
	if err != nil {
		log.Println("关闭日志文件错误", err)
	}
}

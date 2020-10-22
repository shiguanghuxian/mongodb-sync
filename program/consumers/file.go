package consumers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/shiguanghuxian/mongodb-sync/internal/config"
	"github.com/shiguanghuxian/mongodb-sync/internal/logger"
	"github.com/shiguanghuxian/mongodb-sync/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"gopkg.in/natefinch/lumberjack.v2"
)

/* 订阅mongo更新日志，写入日志文件类似oplog，便于数据回滚 */

const (
	BasePath    = "./oplog/" // 输出根目录
	FileMaxSize = 100
)

type FileLogConsumer struct {
	cfg         *config.SyncConfig
	filepath    string
	oplogWriter *lumberjack.Logger
}

func NewFileLogConsumer(cfg *config.SyncConfig) error {
	fileLogConsumer := &FileLogConsumer{
		cfg: cfg,
	}
	err := fileLogConsumer.InitClient(cfg)
	if err != nil {
		return err
	}
	registerConsumer(cfg.GetKey(), fileLogConsumer)
	return nil
}

// 初始化连接
func (fl *FileLogConsumer) InitClient(cfg *config.SyncConfig) error {
	if cfg == nil || cfg.DestinationDb == "" {
		return errors.New("file输出文件名(destination_db)配置不能为空错误")
	}
	// 创建输出目录
	err := os.MkdirAll(BasePath, os.ModePerm)
	if err != nil {
		logger.GlobalLogger.Errorw("oplog根目录创建失败", "err", err, "path", BasePath)
		return err
	}
	fl.filepath = fmt.Sprintf("%s%s-to-%s.oplog", BasePath, cfg.SourceDb, cfg.DestinationDb)
	// 初始化日志输出，定期整理压缩
	fl.oplogWriter = &lumberjack.Logger{
		Filename:  fl.filepath,
		MaxSize:   FileMaxSize,
		LocalTime: true,
		Compress:  true,
	}
	return nil
}

// 销毁连接
func (fl *FileLogConsumer) Disconnect() error {
	if fl.oplogWriter != nil {
		err := fl.oplogWriter.Close()
		if err != nil {
			logger.GlobalLogger.Errorw("关闭oplog文件错误", "err", err, "filepath", fl.filepath)
			return err
		}
	}
	return nil
}

// 处理一条消息
func (fl *FileLogConsumer) HandleData(data *models.ChangeEvent) error {
	if data == nil {
		return errors.New("file处理收到数据为nil")
	}
	log.Println("file处理收到数据", data.Namespace.Db, data.Namespace.Coll)
	js, err := json.Marshal(data)
	if err != nil {
		logger.GlobalLogger.Errorw("file处理收到数据转json错误", "err", err, "data", data)
		return err
	}
	js = append(js, []byte("\n")...)
	n, err := fl.oplogWriter.Write(js)
	if err != nil {
		logger.GlobalLogger.Errorw("file处理收到数据存储到oplog文件错误", "err", err, "data", data)
		return err
	}
	logger.GlobalLogger.Debugw("file处理收到数据，写入成功", "data", data, "n", n)
	return nil
}

// FilterField 删除无用
func (fl *FileLogConsumer) FilterField(collection string, document bson.M) error {
	if document == nil {
		return nil
	}
	for k, _ := range document {
		if !fl.cfg.InCollectionField(collection, k) {
			delete(document, k)
		}
	}
	return nil
}

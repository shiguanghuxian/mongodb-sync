package consumers

import (
	"errors"
	"log"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/shiguanghuxian/mongodb-sync/internal/config"
	"github.com/shiguanghuxian/mongodb-sync/internal/logger"
	"github.com/shiguanghuxian/mongodb-sync/internal/models"
	"go.mongodb.org/mongo-driver/bson"
)

type MysqlConsumer struct {
	debug bool
	cfg   *config.SyncConfig
	db    *gorm.DB
}

func NewMysqlConsumer(cfg *config.SyncConfig, debug bool) error {
	mysqlConsumer := &MysqlConsumer{
		cfg:   cfg,
		debug: debug,
	}
	err := mysqlConsumer.InitClient(cfg)
	if err != nil {
		return err
	}
	registerConsumer(cfg.GetKey(), mysqlConsumer)
	return nil
}

// 初始化连接
func (mc *MysqlConsumer) InitClient(cfg *config.SyncConfig) (err error) {
	if cfg == nil || cfg.DestinationUri == "" {
		return errors.New("mysql目标db链接配置错误")
	}
	mc.db, err = gorm.Open("mysql", cfg.DestinationUri)
	if err != nil {
		return err
	}
	// debug输出sql日志
	if mc.debug {
		mc.db = mc.db.Debug()
	}
	// 更新操作必须有条件
	mc.db.BlockGlobalUpdate(true)
	// 防连接断开
	go func() {
		for {
			func() {
				err := mc.db.DB().Ping()
				if err != nil {
					logger.GlobalLogger.Errorw("定时轮训防止连接断开错误", "err", err, "cfg", mc.cfg)
				}
			}()
			time.Sleep(time.Minute)
		}
	}()
	return nil
}

// 销毁连接
func (mc *MysqlConsumer) Disconnect() error {
	// 注销
	unRegisterConsumer(mc.cfg.GetKey())
	// 关闭连接
	if mc.db != nil {
		err := mc.db.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// 处理一条消息
func (mc *MysqlConsumer) HandleData(data *models.ChangeEvent) error {
	log.Println("mysql处理收到数据", data.Namespace.Db, data.Namespace.Coll, data.Operation)
	return nil
}

// 删除无用
func (mc *MysqlConsumer) FilterField(collection string, document bson.M) error {
	if document == nil {
		return nil
	}
	for k, _ := range document {
		if !mc.cfg.InCollectionField(collection, k) {
			delete(document, k)
		}
	}
	return nil
}

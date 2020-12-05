package consumers

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/shiguanghuxian/mongodb-sync/internal/common"
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

// 初始创建数据表 - 当表不存在时
func (mc *MysqlConsumer) initCreateTable(tableName string) error {
	hasTable := mc.db.HasTable(tableName)
	if hasTable {
		return nil
	}
	createDbSqlPath := fmt.Sprintf("./config/mysql/%s/%s.sql", mc.cfg.DestinationDb, tableName)
	isExist, err := common.PathExists(createDbSqlPath)
	if err != nil {
		logger.GlobalLogger.Errorw("查看创建db sql是否存在错误", "err", err, "db", mc.cfg.DestinationDb, "create_db_sql_path", createDbSqlPath)
		return err
	}
	if isExist {
		body, err := ioutil.ReadFile(createDbSqlPath)
		if err != nil {
			logger.GlobalLogger.Errorw("读取创建db sql文件错误", "err", err, "db", mc.cfg.DestinationDb, "create_db_sql_path", createDbSqlPath)
			return err
		}
		err = mc.db.Exec(string(body)).Error
		if err != nil {
			logger.GlobalLogger.Errorw("执行创建表错误", "err", err, "db", mc.cfg.DestinationDb, "create_db_sql_path", createDbSqlPath)
			return err
		}
	}
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
func (mc *MysqlConsumer) HandleData(data *models.ChangeEvent) (err error) {
	log.Println("mysql处理收到数据", data.Namespace.Db, data.Namespace.Coll, data.Operation)
	tableName := mc.cfg.Collections[data.Namespace.Coll]
	if tableName == "" {
		tableName = data.Namespace.Coll
	}
	err = mc.initCreateTable(tableName)
	if err != nil {
		return err
	}
	db := mc.db.Table(tableName)                              // 保证表名固定
	data.Document["document_key"] = data.DocumentKey.ID.Hex() // 给模型数据添加唯一标识
	switch data.Operation {
	case "insert":
		err = mc.insert(db, data)
	case "update":
		err = mc.update(db, data)
	case "delete":
		err = mc.delete(db, data, tableName)
	case "replace":
		err = mc.replace(db, data, tableName)
	default:
		return errors.New("未知事件类型")
	}
	if err != nil {
		logger.GlobalLogger.Errorw("处理数据错误", "err", err, "data", data, "cfg", mc.cfg)
		return err
	}
	logger.GlobalLogger.Debugw("mongo数据处理成功", "data", data, "cfg", mc.cfg)
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

// 插入数据
func (mc *MysqlConsumer) insert(db *gorm.DB, data *models.ChangeEvent) (err error) {
	err = db.Create(data.Document).Error
	return
}

// 更新数据
func (mc *MysqlConsumer) update(db *gorm.DB, data *models.ChangeEvent) (err error) {
	err = db.Where("document_key = ?", data.Document["document_key"]).Updates(data.Document).Error
	return
}

// 删除
func (mc *MysqlConsumer) delete(db *gorm.DB, data *models.ChangeEvent, typeName string) (err error) {
	err = db.Exec(fmt.Sprintf("DELETE FROM %s WHERE document_key = ?", typeName), data.Document["document_key"]).Error
	return
}

// 查找结果，替换
func (mc *MysqlConsumer) replace(db *gorm.DB, data *models.ChangeEvent, typeName string) (err error) {
	err = mc.delete(db, data, typeName)
	if err != nil {
		return
	}
	err = mc.insert(db, data)
	return
}

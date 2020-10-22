package consumers

import (
	"context"
	"errors"
	"log"
	"os"
	"strings"

	"github.com/olivere/elastic"
	"github.com/shiguanghuxian/mongodb-sync/internal/config"
	"github.com/shiguanghuxian/mongodb-sync/internal/logger"
	"github.com/shiguanghuxian/mongodb-sync/internal/models"
	"go.mongodb.org/mongo-driver/bson"
)

/* elasticsearch目标数据落地 */
type ElasticsearchConsumer struct {
	client *elastic.Client
	cfg    *config.SyncConfig
	Index  string
	Type   string
}

// NewElasticsearchConsumer 创建一个elasticsearch消费对象
func NewElasticsearchConsumer(cfg *config.SyncConfig) error {
	destination := strings.Split(cfg.DestinationDb, "/")
	if len(destination) != 2 || destination[0] == "" || destination[1] == "" {
		return errors.New("elasticsearch 目标index/type配置错误(destination_db)")
	}
	elasticsearchConsumer := &ElasticsearchConsumer{
		cfg:   cfg,
		Index: destination[0],
		Type:  destination[1],
	}
	err := elasticsearchConsumer.InitClient(cfg)
	if err != nil {
		return err
	}
	registerConsumer(cfg.GetKey(), elasticsearchConsumer)
	return nil
}

// 初始化连接
func (ec *ElasticsearchConsumer) InitClient(cfg *config.SyncConfig) error {
	if cfg == nil || cfg.DestinationUri == "" {
		return errors.New("elasticsearch目标db链接配置错误")
	}
	address := strings.Split(cfg.DestinationUri, ";")
	esClient, err := elastic.NewClient(elastic.SetURL(address...), elastic.SetSniff(false), elastic.SetErrorLog(log.New(os.Stdout, "ES-ERROR: ", 0)))
	if err != nil {
		return err
	}
	ec.client = esClient
	return nil
}

// 销毁连接
func (ec *ElasticsearchConsumer) Disconnect() error {
	// 注销
	unRegisterConsumer(ec.cfg.GetKey())
	return nil
}

// 处理一条消息
func (ec *ElasticsearchConsumer) HandleData(data *models.ChangeEvent) error {
	log.Println("elasticsearch处理收到数据", data.Namespace.Db, data.Namespace.Coll, data.Operation)
	var err error
	// 根据操作不同处理
	switch data.Operation {
	case "insert":
		err = ec.insert(data)
	case "update":
		err = ec.update(data)
	case "delete":
		err = ec.delete(data)
	case "replace":
		err = ec.replace(data)
	default:
		return errors.New("未知事件类型")
	}
	if err != nil {
		logger.GlobalLogger.Errorw("处理数据错误", "err", err, "data", data, "cfg", ec.cfg)
		return err
	}
	logger.GlobalLogger.Debugw("elasticsearch数据处理成功", "data", data, "cfg", ec.cfg)
	return nil
}

// FilterField 删除无用
func (ec *ElasticsearchConsumer) FilterField(collection string, document bson.M) error {
	if document == nil {
		return nil
	}
	for k, _ := range document {
		if !ec.cfg.InCollectionField(collection, k) {
			delete(document, k)
		}
	}
	return nil
}

// 插入es一条数据 - 未来可优化为批量插入 - 需要考虑时间和数据一致性
func (ec *ElasticsearchConsumer) insert(data *models.ChangeEvent) error {
	bulkRequest := ec.client.Bulk()
	bulkRequest.Add(elastic.NewBulkUpdateRequest().Index(ec.Index).Type(ec.Type).Id(data.DocumentKey.ID.Hex()).Doc(data.Document).DocAsUpsert(true))
	ctx, cancel := context.WithTimeout(context.Background(), TimeoutCtx)
	defer cancel()
	bulkResponse, err := bulkRequest.Do(ctx)
	if err != nil {
		logger.GlobalLogger.Errorw("数据插入elasticsearch错误", "err", err, "data", data, "cfg", ec.cfg)
		return err
	}
	log.Println("创建文档数", len(bulkResponse.Succeeded()))
	return nil
}

// 更新数据
func (ec *ElasticsearchConsumer) update(data *models.ChangeEvent) error {
	bulkRequest := ec.client.Bulk()
	bulkRequest.Add(elastic.NewBulkUpdateRequest().Index(ec.Index).Type(ec.Type).Id(data.DocumentKey.ID.Hex()).Upsert(data.Document))
	ctx, cancel := context.WithTimeout(context.Background(), TimeoutCtx)
	defer cancel()
	bulkResponse, err := bulkRequest.Do(ctx)
	if err != nil {
		logger.GlobalLogger.Errorw("更新elasticsearch数据错误", "err", err, "data", data, "cfg", ec.cfg)
		return err
	}
	log.Println("更新文档数", len(bulkResponse.Updated()))
	return nil
}

// 删除一条数据
func (ec *ElasticsearchConsumer) delete(data *models.ChangeEvent) error {
	bulkRequest := ec.client.Bulk()
	bulkRequest.Add(elastic.NewBulkDeleteRequest().Index(ec.Index).Type(ec.Type).Id(data.DocumentKey.ID.Hex()))
	ctx, cancel := context.WithTimeout(context.Background(), TimeoutCtx)
	defer cancel()
	bulkResponse, err := bulkRequest.Do(ctx)
	if err != nil {
		logger.GlobalLogger.Errorw("删除elasticsearch数据错误", "err", err, "data", data, "cfg", ec.cfg)
		return err
	}
	log.Println("删除文档数", len(bulkResponse.Deleted()))
	return nil
}

// 替换全部文档内容时，先删后加
func (ec *ElasticsearchConsumer) replace(data *models.ChangeEvent) error {
	err := ec.delete(data)
	if err != nil {
		return err
	}
	err = ec.insert(data)
	return err
}

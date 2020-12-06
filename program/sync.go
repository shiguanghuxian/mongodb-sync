package program

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/shiguanghuxian/mongodb-sync/internal/config"
	"github.com/shiguanghuxian/mongodb-sync/internal/logger"
	"github.com/shiguanghuxian/mongodb-sync/internal/models"
	"github.com/shiguanghuxian/mongodb-sync/internal/mongodb"
	"github.com/shiguanghuxian/mongodb-sync/program/consumers"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

// 同步源mongo数据到目标mongo
func (p *Program) sync() {
	p.collectionChan = make(map[string]chan *models.ChangeEvent)
	// 对每一个collection初始化一个chan
	for _, v := range p.cfg.Sync {
		v := v
		key := v.GetKey()
		// 一个db一个chan
		if p.collectionChan[key] != nil {
			continue
		}
		documentChan := make(chan *models.ChangeEvent, 1)
		go p.consumer(key, documentChan)
		// 保存
		p.collectionChan[key] = documentChan

		// 处理每一个db的数据订阅
		p.producerDbWatch(v)
	}
}

// 处理一个db的数据订阅
func (p *Program) producerDbWatch(syncCfg *config.SyncConfig) {
	if syncCfg == nil {
		return
	}
	logger.GlobalLogger.Infow("处理一个db的数据订阅", "cfg", syncCfg)
	log.Println("处理一个db的数据订阅", syncCfg)
	key := syncCfg.GetKey()
	documentChan := p.collectionChan[key]
	if documentChan == nil {
		logger.GlobalLogger.Errorw("出现了nil的chan", "key", key)
		return
	}
	// 订阅配置
	configOptions := new(options.ChangeStreamOptions)
	configOptions.SetFullDocument("updateLookup")
	// 从上次结束位置开始订阅
	if rtStr, ok := p.GetLastEventIds(syncCfg.GetKey()); ok && len(rtStr) > 0 {
		rt := &bsonx.Doc{}
		err := bson.Unmarshal(rtStr, rt)
		if err != nil {
			logger.GlobalLogger.Errorw("解析上次结束位置错误", "err", err, "source_db", syncCfg.SourceDb, "destination_db", syncCfg.DestinationDb, "rtStr", rtStr)
		} else {
			configOptions.SetResumeAfter(rt)
		}
	}

	// 订阅超时
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if p.cfg.Mongo.SourceVersion >= 4.0 {
		cursor, err := mongodb.SourceClient.Database(syncCfg.SourceDb).Watch(ctx, bson.A{}, configOptions)
		if err != nil {
			logger.GlobalLogger.Errorw("订阅db错误", "err", err, "source_db", syncCfg.SourceDb, "destination_db", syncCfg.DestinationDb)
			return
		}
		go p.producer(cursor, syncCfg, documentChan)
	} else {
		for sourceCollection, _ := range syncCfg.Collections {
			cursor, err := mongodb.SourceClient.Database(syncCfg.SourceDb).Collection(sourceCollection).Watch(ctx, bson.A{}, configOptions)
			if err != nil {
				logger.GlobalLogger.Errorw("订阅db错误", "err", err, "source_db", syncCfg.SourceDb, "destination_db", syncCfg.DestinationDb, "source_collection", sourceCollection)
				return
			}
			go p.producer(cursor, syncCfg, documentChan)
		}
	}
}

// 从源读取数据
func (p *Program) producer(cursor *mongo.ChangeStream, syncCfg *config.SyncConfig, documentChan chan *models.ChangeEvent) {
	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()
	ctx := context.TODO()
	for cursor.Next(ctx) {
		log.Println("监听db变化", syncCfg.SourceDb, "id", cursor.ID())
		if cursor.ID() == 0 {
			logger.GlobalLogger.Errorw("mongo客户端订阅对象已经关闭", "syncCfg", syncCfg)
			break
		}
		if err := cursor.Err(); err != nil {
			logger.GlobalLogger.Errorw("订阅mongo数据变化错误", "err", err, "syncCfg", syncCfg)
			break
		}
		changeEvent := new(models.ChangeEvent)
		if err := cursor.Decode(changeEvent); err != nil {
			logger.GlobalLogger.Errorw("解析mongo订阅事件错误", "err", err, "syncCfg", syncCfg, "_id", cursor.ID())
			continue
		}

		// 存储本次数据变更到lastEventIds
		lastEventIdByte, err := bson.Marshal(changeEvent.ID)
		if err != nil {
			logger.GlobalLogger.Errorw("序列号最后变更id错误", "err", err, "syncCfg", syncCfg, "_id", cursor.ID(), "lastEventIdByte", string(lastEventIdByte))
		}
		// log.Println("记录最后事件id", string(lastEventIdByte))
		if len(lastEventIdByte) > 0 {
			p.SetLastEventIds(syncCfg.GetKey(), lastEventIdByte)
		}

		// js, _ := json.Marshal(changeEvent)
		// log.Println("监听db变化", syncCfg.SourceDb, "data", string(js))
		documentChan <- changeEvent
	}
}

// 消费源数据
func (p *Program) consumer(key string, documentChan chan *models.ChangeEvent) {
	if documentChan == nil {
		return
	}
	for {
		data := <-documentChan
		// log.Println("哈哈", data)
		consumers.HandleData(key, data)
	}
}

// GetLastEventIds 读取上次结束位置lastEventId
func (p *Program) GetLastEventIds(key string) ([]byte, bool) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	val, ok := p.lastEventIds[key]
	return val, ok
}

// SetLastEventIds 设置最后监听事件id
func (p *Program) SetLastEventIds(key string, val []byte) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.lastEventIds[key] = val
}

// GetLastEventIdsToJsonBytes 获取json字符
func (p *Program) GetLastEventIdsToJsonBytes() ([]byte, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	js, err := json.Marshal(p.lastEventIds)
	return js, err
}

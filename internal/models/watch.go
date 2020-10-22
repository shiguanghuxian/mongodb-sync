package models

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

// mongo订阅消息对象
type ChangeEvent struct {
	ID                bsonx.Doc   `bson:"_id" json:"_id"`
	Operation         string      `bson:"operationType" json:"operation"`
	Document          bson.M      `bson:"fullDocument" json:"document"`
	Namespace         namespace   `bson:"ns" json:"namespace"`
	NewCollectionName bson.M      `bson:"to" json:"new_collection_name"`
	DocumentKey       documentKey `bson:"documentKey" json:"document_key"`
	Updates           bson.M      `bson:"updateDescription" json:"updates"`
	ClusterTime       interface{} `bson:"clusterTime" json:"cluster_time"`
	Transaction       int64       `bson:"txnNumber" json:"transaction"`
	SessionID         bson.M      `bson:"lsid" json:"session_id"`
}

type documentKey struct {
	ID primitive.ObjectID `bson:"_id" json:"_id"`
}

type namespace struct {
	Coll string `bson:"coll" json:"coll"`
	Db   string `bson:"db" json:"db"`
}

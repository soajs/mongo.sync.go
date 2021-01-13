/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */

package source

import (
	"context"
	"github.com/soajs/mongo.sync.go/mongodb"
	"github.com/soajs/mongo.sync.go/settings"
	"github.com/soajs/mongo.sync.go/token"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

type ChangeEvent struct {
	ID            token.Data             `bson:"_id" json:"_id"`
	FullDocument  map[string]interface{} `bson:"fullDocument" json:"fullDocument"`
	OperationType string                 `bson:"operationType" json:"operationType"`
	DocumentKey   struct {
		Id primitive.ObjectID `bson:"_id" json:"_id"`
	} `bson:"documentKey" json:"documentKey"`
	Ns struct {
		Coll string `bson:"coll" json:"coll"`
		Db   string `bson:"db" json:"db"`
	} `bson:"ns" json:"ns"`
}

type Params struct {
	DbName  string
	ColName string
	Time    string
	Token   token.Data
}

type Source struct {
	client         *mongo.Client
	ctx            context.Context
	operationTypes []string
}

func NewSource(ctx context.Context) *Source {
	return &Source{ctx: ctx}
}

func (o *Source) Connect(config settings.MongoSource) error {
	o.operationTypes = config.Stream
	client, err := mongodb.Connect(o.ctx, config.Uri)
	if err != nil {
		return err
	}
	o.client = client
	return nil
}

func (o *Source) GetStream(param *Params) (*mongo.ChangeStream, error) {
	db := o.client.Database(param.DbName)
	col := db.Collection(param.ColName)
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if param.Token.Data != nil {
		opts.SetStartAfter(param.Token)
	} else if param.Time != "" {
		layout := "2006-01-02"
		t, err := time.Parse(layout, param.Time)
		if err != nil {
			log.Println(err)
		} else {
			pt := primitive.Timestamp{T: uint32(t.Unix())}
			opts.SetStartAtOperationTime(&pt)
		}
	}

	pipeline := []bson.M{{"$match": bson.M{"operationType": bson.M{"$in": o.operationTypes}}}, {"$project": bson.M{"documentKey": true, "operationType": true, "ns": true, "fullDocument": true}}}

	stream, err := col.Watch(o.ctx, pipeline, opts)

	return stream, err
}

func (o *Source) GetClone(param *Params) (*mongo.Cursor, error) {
	db := o.client.Database(param.DbName)
	col := db.Collection(param.ColName)

	layout := "2006-01-02"
	t, err := time.Parse(layout, param.Time)
	if err != nil {
		return nil, err
	} else {
		b := primitive.NewObjectIDFromTimestamp(t)
		cursor, err := col.Find(o.ctx, bson.M{"_id": bson.M{"$lt": b}})

		return cursor, err
	}
}

func (o *Source) GetCount(param *Params) (int64, error) {
	db := o.client.Database(param.DbName)
	col := db.Collection(param.ColName)
	count, err := col.CountDocuments(o.ctx, nil)

	return count, err
}

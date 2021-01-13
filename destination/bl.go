/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */

package destination

import (
	"context"
	"github.com/soajs/mongo.sync.go/mongodb"
	"github.com/soajs/mongo.sync.go/settings"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Destination struct {
	client *mongo.Client
	ctx    context.Context
}
type Params struct {
	DbName  string
	ColName string
	Id      primitive.ObjectID
	Record  map[string]interface{}
}

func NewSource(ctx context.Context) *Destination {
	return &Destination{ctx: ctx}
}

func (o *Destination) Connect(config settings.MongoUri) error {
	client, err := mongodb.Connect(o.ctx, config.Uri)
	if err != nil {
		return err
	}
	o.client = client
	return nil
}

func (o *Destination) Drop(param *Params) error {
	db := o.client.Database(param.DbName)
	col := db.Collection(param.ColName)

	err := col.Drop(o.ctx)
	return err
}

func (o *Destination) Add(param *Params) (interface{}, bool, error) {
	db := o.client.Database(param.DbName)
	col := db.Collection(param.ColName)

	result, err := col.InsertOne(o.ctx, param.Record)
	if err != nil {
		return nil, false, err
	} else {
		return result.InsertedID, result.InsertedID != nil, nil
	}
}

func (o *Destination) Upsert(param *Params) (bool, error) {
	db := o.client.Database(param.DbName)
	col := db.Collection(param.ColName)
	filter := bson.M{"_id": param.Id}
	update := bson.M{"$set": param.Record}
	opts := options.Update().SetUpsert(true)

	result, err := col.UpdateOne(o.ctx, filter, update, opts)
	if err != nil {
		return false, err
	} else {
		return result.MatchedCount == 1 || result.ModifiedCount == 1 || result.UpsertedCount == 1, nil
	}
}

func (o *Destination) Delete(param *Params) (bool, error) {
	db := o.client.Database(param.DbName)
	col := db.Collection(param.ColName)
	filter := bson.M{"_id": param.Id}
	opts := options.Delete()

	result, err := col.DeleteOne(o.ctx, filter, opts)
	if err != nil {
		return false, err
	} else {
		return result.DeletedCount == 1, nil
	}
}

/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */

package token

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"soajs/mongo.sync.go/mongodb"
	"soajs/mongo.sync.go/settings"
	"time"
)

type Token struct {
	client  *mongo.Client
	ctx     context.Context
	dbName  string
	colName string
}

func NewToken(ctx context.Context) *Token {
	return &Token{ctx: ctx, dbName: "token", colName: "tokens"}
}

func (o *Token) Connect(config settings.MongoUri) error {
	client, err := mongodb.Connect(o.ctx, config.Uri)
	if err != nil {
		return err
	}
	o.client = client
	return nil
}

func (o *Token) Get(id string) (*Document, error) {
	db := o.client.Database(o.dbName)
	col := db.Collection(o.colName)
	var result Document

	err := col.FindOne(o.ctx, bson.M{"_id": id}).Decode(&result)

	if err != nil {
		if err.Error() == "mongo: no documents in result" {
			return nil, nil
		}
		return nil, err
	}
	return &result, nil
}

func (o *Token) Save(id string, d *Data) (bool, error) {
	db := o.client.Database(o.dbName)
	col := db.Collection(o.colName)
	filter := bson.M{"_id": id}
	update := bson.M{"$set": bson.M{"lastModifiedDate": time.Now(), "token": &d.Data}}
	opts := options.Update().SetUpsert(true)

	result, err := col.UpdateOne(o.ctx, filter, update, opts)

	return result.ModifiedCount == 1 || result.UpsertedCount == 1, err
}

/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */

package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/soajs/mongo.sync.go/destination"
	"github.com/soajs/mongo.sync.go/settings"
	"github.com/soajs/mongo.sync.go/source"
	"github.com/soajs/mongo.sync.go/token"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
)

type Doc struct {
	Id primitive.ObjectID `bson:"_id" json:"_id"`
}

var streamOperations = []string{"insert", "update", "replace"}
var tokenId = "TOKEN_ID"

func inArray(val interface{}, array interface{}) (exists bool) {
	exists = false

	switch reflect.TypeOf(array).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(array)

		for i := 0; i < s.Len(); i++ {
			if reflect.DeepEqual(val, s.Index(i).Interface()) == true {
				exists = true
				return
			}
		}
	}

	return
}

func getTime(opt *settings.MongoSync) string {
	opsTime := os.Getenv("SOAJS_MONGO_SYNC_OPSTIME")
	if opsTime == "1" {
		return time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	} else if opsTime == "2" {
		return opt.FirstOpTime
	} else {
		return ""
	}
}

func runSyncStream(ctx context.Context, wg *sync.WaitGroup, sourceStream *mongo.ChangeStream, t *token.Token, d *destination.Destination, collection *settings.MongoCollections, fatalErrors chan error) {
	defer sourceStream.Close(ctx)
	defer wg.Done()
	for sourceStream.Next(ctx) {
		var ce source.ChangeEvent
		sourceStream.Decode(&ce)
		if ce.OperationType == "delete" {
			var params destination.Params
			params.DbName = collection.D.DbName
			params.ColName = collection.D.ColName
			params.Id = ce.DocumentKey.Id
			result, err := d.Delete(&params)
			if err != nil {
				fatalErrors <- err
				break
			} else if !result {
				err = errors.New(fmt.Sprintf("unable to delete document with id %s", params.Id))
				fatalErrors <- err
				break
			}
			fmt.Printf("delete %v\n", ce.OperationType)
			fmt.Printf("%v\n", ce.Ns)
		} else if inArray(ce.OperationType, streamOperations) {
			var params destination.Params
			params.DbName = collection.D.DbName
			params.ColName = collection.D.ColName
			params.Record = ce.FullDocument
			params.Id = ce.DocumentKey.Id
			result, err := d.Upsert(&params)
			if err != nil {
				fatalErrors <- err
				break
			} else if !result {
				err = errors.New(fmt.Sprintf("unable to upsert document with id %s", params.Id))
				fatalErrors <- err
				break
			}
		}
		var data token.Data
		var id = fmt.Sprintf("%s_%s_%s", collection.S.DbName, collection.S.ColName, tokenId)
		data.Data = ce.ID
		_, err := t.Save(id, &data)
		if err != nil {
			log.Println(err)
		}
	}
}

func executeSync(ctx context.Context, collection *settings.MongoCollections, opt *settings.MongoSync, t *token.Token, s *source.Source, d *destination.Destination, wg0 *sync.WaitGroup, skipResume bool) {
	fatalErrors := make(chan error)
	wgDone := make(chan bool)

	var wg1 sync.WaitGroup
	wg1.Add(1)

	var sourceStream *mongo.ChangeStream

	go func() {
		log.Println(fmt.Sprintf("Synchronizing from : %s.%s to %s.%s", collection.S.DbName, collection.S.ColName, collection.D.DbName, collection.D.ColName))

		var wg2 sync.WaitGroup

		var opTime string
		var id = fmt.Sprintf("%s_%s_%s", collection.S.DbName, collection.S.ColName, tokenId)
		colToken, err := t.Get(id)
		if err != nil {
			fatalErrors <- err
			wg1.Done()
		} else {
			if colToken != nil {
				log.Println(fmt.Sprintf("Token: %s", colToken.Token.Data))
			} else {
				opTime = getTime(opt)
				log.Println(fmt.Sprintf("Date: %s", opTime))
			}

			var params source.Params
			params.DbName = collection.S.DbName
			params.ColName = collection.S.ColName
			if colToken != nil {
				params.Token = colToken.Token
			}
			params.Time = opTime
			if skipResume {
				log.Println("Skipping resume since it is no longer in oplog ...")
				params.Token.Data = nil
				params.Time = ""
			}
			sourceStream, err = s.GetStream(&params)
			if err != nil {
				fatalErrors <- err
				wg1.Done()
			} else {
				wg2.Add(1)
				go runSyncStream(ctx, &wg2, sourceStream, t, d, collection, fatalErrors)
				wg2.Wait()
				wg1.Done()
			}
		}
	}()

	go func() {
		wg1.Wait()
		close(wgDone)
	}()

	select {
	case <-wgDone:
		close(fatalErrors)
		wg0.Done()
		break
	case err := <-fatalErrors:
		close(fatalErrors)
		log.Println(err)
		if sourceStream != nil {
			sourceStream.Close(ctx)
		}
		if strings.Contains(err.Error(), "no longer be in the oplog") {
			executeSync(ctx, collection, opt, t, s, d, wg0, true)
		} else {
			log.Println("Will try to execute sync again after 5 minute")
			time.AfterFunc(5*time.Minute, func() {
				executeSync(ctx, collection, opt, t, s, d, wg0, false)
			})
		}
	}
}

func runCopyStream(ctx context.Context, wg *sync.WaitGroup, cursor *mongo.Cursor, d *destination.Destination, collection *settings.MongoCollections, fatalErrors chan error) {
	defer cursor.Close(ctx)
	defer wg.Done()
	var counter = 0
	for cursor.Next(ctx) {
		counter = counter + 1
		var rec bson.M
		err := cursor.Decode(&rec)
		if err != nil {
			fatalErrors <- err
			break
		}

		var doc Doc
		err = cursor.Decode(&doc)
		if err != nil {
			fatalErrors <- err
			break
		}
		var params destination.Params
		params.DbName = collection.D.DbName
		params.ColName = collection.D.ColName
		params.Id = doc.Id
		params.Record = rec
		result, err := d.Upsert(&params)
		if err != nil {
			fatalErrors <- err
			break
		} else if !result {
			err = errors.New(fmt.Sprint("unable to add document with id ", doc.Id))
			fatalErrors <- err
			break
		}
	}
	log.Println(fmt.Sprintf("Copying from : %s.%s to %s.%s - Done with count [%d]", collection.S.DbName, collection.S.ColName, collection.D.DbName, collection.D.ColName, counter))
}

func executeCopy(ctx context.Context, collection *settings.MongoCollections, opt *settings.MongoSync, t *token.Token, s *source.Source, d *destination.Destination, wg0 *sync.WaitGroup) {
	fatalErrors := make(chan error)
	wgDone := make(chan bool)

	var wg1 sync.WaitGroup
	wg1.Add(1)

	var cursor *mongo.Cursor

	go func() {
		var opTime string
		var id = fmt.Sprintf("%s_%s_%s", collection.S.DbName, collection.S.ColName, tokenId)
		colToken, err := t.Get(id)
		if err != nil {
			fatalErrors <- err
			wg1.Done()
		} else {
			if colToken != nil {
				wg1.Done()
			} else {
				log.Println(fmt.Sprintf("Copying from : %s.%s to %s.%s", collection.S.DbName, collection.S.ColName, collection.D.DbName, collection.D.ColName))
				opTime = getTime(opt)
				if opTime != "" {
					var wg2 sync.WaitGroup
					var params source.Params
					params.DbName = collection.S.DbName
					params.ColName = collection.S.ColName
					params.Time = opTime
					cursor, err = s.GetClone(&params)
					if err != nil {
						fatalErrors <- err
						wg1.Done()
					} else {
						var params destination.Params
						params.DbName = collection.S.DbName
						params.ColName = collection.S.ColName
						if collection.Drop {
							err := d.Drop(&params)
							if err != nil {
								log.Println(err)
							}
						}
						wg2.Add(1)
						go runCopyStream(ctx, &wg2, cursor, d, collection, fatalErrors)
						wg2.Wait()
						wg1.Done()
					}
				} else {
					log.Println("Cannot copy collection without ops time, skipping copying")
					wg1.Done()
				}
			}
		}
	}()

	go func() {
		wg1.Wait()
		close(wgDone)
	}()

	select {
	case <-wgDone:
		close(fatalErrors)
		wg0.Done()
		break
	case err := <-fatalErrors:
		close(fatalErrors)
		log.Println(err)
		if cursor != nil {
			cursor.Close(ctx)
		}
		log.Println("Will try to execute copy again after 5 minute")
		time.AfterFunc(5*time.Minute, func() {
			executeCopy(ctx, collection, opt, t, s, d, wg0)
		})
	}
}

func main() {
	log.Println("Starting mongo synchronization ...")
	ctx := context.Background()
	opt, err := settings.GetMongoSync()
	if err != nil {
		log.Fatal(err)
	}
	t := token.NewToken(ctx)
	tErr := t.Connect(opt.Token)
	if tErr != nil {
		log.Fatal(tErr)
	}
	d := destination.NewSource(ctx)
	dErr := d.Connect(opt.Destination)
	if dErr != nil {
		log.Fatal(dErr)
	}
	s := source.NewSource(ctx)
	sErr := s.Connect(opt.Source)
	if sErr != nil {
		log.Fatal(sErr)
	}

	var wgCopy sync.WaitGroup
	for i := 0; i < len(opt.Collections); i++ {
		if opt.Collections[i].Copy {
			wgCopy.Add(1)
			go executeCopy(ctx, &opt.Collections[i], opt, t, s, d, &wgCopy)
		}
	}
	wgCopy.Wait()

	var wgSync sync.WaitGroup
	for i := 0; i < len(opt.Collections); i++ {
		wgSync.Add(1)
		go executeSync(ctx, &opt.Collections[i], opt, t, s, d, &wgSync, false)

	}
	wgSync.Wait()
}

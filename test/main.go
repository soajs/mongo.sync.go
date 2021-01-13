/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */

package main

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"log"
	"time"
)

func main() {
	myTime := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	log.Println(myTime)
	layout := "2006-01-02"
	t, err := time.Parse(layout, myTime)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Println(t)
		log.Println(t.Unix())
		b := primitive.NewObjectIDFromTimestamp(t)
		log.Println(b)
	}

}

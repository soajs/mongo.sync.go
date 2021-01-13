/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */

package settings

type MongoSync struct {
	FirstOpTime string             `json:"firstOpTime"`
	Token       MongoUri           `json:"token"`
	Source      MongoSource        `json:"source"`
	Destination MongoUri           `json:"destination"`
	Collections []MongoCollections `json:"collections"`
}

type MongoCollections struct {
	S    MongoColInfo `json:"s"`
	D    MongoColInfo `json:"d"`
	Copy bool         `json:"copy"`
	Drop bool         `json:"drop"`
}

type MongoColInfo struct {
	DbName  string `json:"dbName"`
	ColName string `json:"colName"`
}

type MongoSource struct {
	Uri    string   `json:"uri"`
	Stream []string `json:"stream"`
}

type MongoUri struct {
	Uri string `json:"uri"`
}

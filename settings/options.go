/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */

package settings

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
)

func GetMongoSync() (*MongoSync, error) {
	var options MongoSync
	err := options.getAsFile()
	if err != nil {
		if os.IsNotExist(err) {
			err = options.getAsEnv()
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	err = options.Validate()
	if err != nil {
		return nil, err
	}
	return &options, nil
}

func (o *MongoSync) Validate() error {
	if len(o.Collections) == 0 {
		return errors.New("could not find [collections] in your options, collections is <required>")
	}
	return nil
}

func (o *MongoSync) getAsEnv() error {
	jsonEnv := os.Getenv(EnvMongoSync)
	err := json.Unmarshal([]byte(jsonEnv), &o)
	if err != nil {
		return err
	}
	return err
}

func (o *MongoSync) getAsFile() error {
	filename := os.Getenv(EnvMongoSync)
	jsonFile, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	err = json.Unmarshal(byteValue, &o)
	if err != nil {
		return err
	}
	return err
}

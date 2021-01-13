/**
 * @license
 * Copyright SOAJS All Rights Reserved.
 *
 * Use of this source code is governed by an Apache license that can be
 * found in the LICENSE file at the root of this repository
 */

package token

import "time"

type Document struct {
	ID               string    `json:"_id" bson:"_id"`
	LastModifiedDate time.Time `json:"lastModifiedDate" bson:"lastModifiedDate"`
	Token            Data      `json:"token" bson:"token"`
}

type Data struct {
	Data interface{} `json:"_data" bson:"_data"`
}

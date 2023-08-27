/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package pebblestore

import (
	"github.com/cockroachdb/pebble"
	"reflect"
)

func OpenDatabase(dataDir string, opts *pebble.Options) (*pebble.DB, error) {
	return pebble.Open(dataDir, opts)
}

func ObjectType() reflect.Type {
	return PebbleStoreClass
}

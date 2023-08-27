/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package pebblestore

import (
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
)

var (

	WriteOptions = pebble.NoSync

	ErrInvalidFormat     = errors.New("invalid format")
	ErrOperationCanceled = errors.New("operation was canceled")
)


// SPDX-FileCopyrightText: 2026 2M Production Electrique
// SPDX-License-Identifier: AGPL-3.0-or-later
//go:build !libopus

package media

import "errors"

type OpusDecoder struct{}

func NewOpusDecoder() (*OpusDecoder, error) {
	return nil, errors.New("Opus support not compiled; rebuild with -tags libopus and libopus-dev installed")
}

func (d *OpusDecoder) Close() {}

func (d *OpusDecoder) Decode(payload []byte) ([]int16, error) {
	return nil, errors.New("Opus support not compiled")
}

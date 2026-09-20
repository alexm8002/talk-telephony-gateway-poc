//go:build libopus

package media

/*
#cgo pkg-config: opus
#include <opus/opus.h>
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

type OpusDecoder struct {
	ptr *C.OpusDecoder
}

func NewOpusDecoder() (*OpusDecoder, error) {
	var cerr C.int
	p := C.opus_decoder_create(48000, 1, &cerr)
	if p == nil || cerr != C.OPUS_OK {
		return nil, fmt.Errorf("opus_decoder_create: %d", int(cerr))
	}
	return &OpusDecoder{ptr: p}, nil
}

func (d *OpusDecoder) Close() {
	if d != nil && d.ptr != nil {
		C.opus_decoder_destroy(d.ptr)
		d.ptr = nil
	}
}

func (d *OpusDecoder) Decode(payload []byte) ([]int16, error) {
	if d == nil || d.ptr == nil {
		return nil, errors.New("opus decoder is closed")
	}
	if len(payload) == 0 {
		return nil, nil
	}
	// 120 ms at 48 kHz is the maximum Opus frame duration.
	pcm := make([]int16, 5760)
	n := C.opus_decode(
		d.ptr,
		(*C.uchar)(unsafe.Pointer(&payload[0])),
		C.opus_int32(len(payload)),
		(*C.opus_int16)(unsafe.Pointer(&pcm[0])),
		C.int(len(pcm)),
		0,
	)
	if n < 0 {
		return nil, fmt.Errorf("opus_decode: %d", int(n))
	}
	return pcm[:int(n)], nil
}

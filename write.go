// Copyright 2014 Tamás Gulácsi
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

// Package sz implements "Snappy-framed" streaming Reader/Writer with Snappy.
//
// See https://code.google.com/p/snappy/source/browse/trunk/framing_format.txt
package sz

import (
	"encoding/binary"
	"errors"
	"io"

	"code.google.com/p/leveldb-go/leveldb/crc"
	"code.google.com/p/snappy-go/snappy"
)

var streamFirstChunk = []byte{0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59}

const maxUncomprLength = 65536

// comprLen = 32 + srcLen + srcLen/6 => srcLen = (comprLen * 6 - 32*6) / 7
const maxComprLength = (maxUncomprLength*6 - 32*6) / 7

// Writer implements a Snappy-framed compressing stream io.Writer.
type Writer struct {
	w          io.Writer
	raw, compr []byte // scratchpads
}

// NewWriter creates a new Writer, which will write compressed data to the
// underlying io.Writer.
//
func NewWriter(w io.Writer) (*Writer, error) {
	// Stream identifier 0xff + LE length + "sNaPpY" in ASCII.
	_, err := w.Write(streamFirstChunk)
	if err != nil {
		return nil, err
	}
	return &Writer{
		w:     w,
		compr: make([]byte, 0, maxComprLength),
		raw:   make([]byte, 0, maxUncomprLength),
	}, nil
}

// Write writes a compressed form of p to the underlying io.Writer.
// The compressed bytes are not necessarily flushed until the Writer is closed.
func (z *Writer) Write(p []byte) (int, error) {
	z.raw = append(z.raw, p...)
	if len(z.raw) < maxComprLength {
		return len(p), nil
	}
	buf := z.raw
	for len(buf) >= maxComprLength {
		if err := z.writeCompressedChunk(buf[:maxComprLength]); err != nil {
			return len(p), err
		}
		buf = buf[maxComprLength:]
	}
	// don't let the underlying array grow too big
	z.raw = z.raw[:len(buf)]
	copy(z.raw, buf)

	return len(p), nil
}

func (z *Writer) writeCompressedChunk(raw []byte) error {
	var err error
	if len(raw) > maxComprLength {
		return errors.New("chunk too big")
	}
	z.compr, err = snappy.Encode(z.compr[:cap(z.compr)], raw)
	if err != nil {
		return err
	}
	if len(z.compr) > len(raw) {
		return z.writeUncompressedChunk(raw)
	}

	return z.writeChunk(0x00, raw, crc.New(raw).Value())
}

func (z *Writer) writeUncompressedChunk(raw []byte) error {
	return z.writeChunk(0x01, raw, crc.New(raw).Value())
}

// writeChunk writes a specified chunk, about p with u as crc.
//
// The file consists solely of chunks, lying back-to-back with no padding
// in between. Each chunk consists first a single byte of chunk identifier,
// then a three-byte little-endian length of the chunk in bytes (from 0 to
// 16777215, inclusive), and then the data if any. The four bytes of chunk
// header is not counted in the data length.
func (z *Writer) writeChunk(flag byte, p []byte, u uint32) error {
	var prefix [8]byte
	// prefix[0] = 0x00   =  compressed data
	n := uint32(len(p)) + 4 // data length + checksum length
	prefix[1] = byte(n)
	prefix[2] = byte(n >> 8)
	prefix[3] = byte(n >> 16)
	binary.LittleEndian.PutUint32(prefix[4:8], u)
	if _, err := z.w.Write(prefix[:]); err != nil {
		return err
	}
	if _, err := z.w.Write(p); err != nil {
		return err
	}
	return nil
}

func maxDecodedLen(comprLen int) int {
	// comprLen = 32 + srcLen + srcLen/6 => srcLen = (comprLen * 6 - 32*6) / 7
	return (comprLen*6 - 32*6) / 7
}

// Flish flushes any pending compressed data to the underlying writer.
func (z *Writer) Flush() error {
	if len(z.raw) == 0 {
		return nil
	}
	var err error
	if z.compr, err = snappy.Encode(z.compr, z.raw); err != nil {
		return err
	}
	if _, err = z.w.Write(z.compr); err != nil {
		return err
	}
	z.raw = z.raw[:0]
	return nil
}

// Close flushes the remaining unwritten data to the underlying io.Writer,
// but does not close the underlying io.Writer.
func (z *Writer) Close() error {
	return z.Flush()
}

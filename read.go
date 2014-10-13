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

package sz

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"

	"code.google.com/p/leveldb-go/leveldb/crc"
	"code.google.com/p/snappy-go/snappy"
)

const maxChunkLength = 65536 + 4 + 4

// ErrNotSnappy is returned when the underlying stream is not Snappy-framed.
var ErrNotSnappy = errors.New("not snappy")

// ErrUnskippableChunk is for reserved unskippable, undecodable chunks
var ErrUnskippableChunk = errors.New("unskippable chunk")

// ErrBadChecksum is for CRC mismatch
var ErrBadChecksum = errors.New("bad checksum")

// Reader wraps the underlying reader and decompresses it.
type Reader struct {
	r              io.Reader
	buf, remainder []byte
}

func NewReader(r io.Reader) (*Reader, error) {
	first := len(streamFirstChunk)
	buf := make([]byte, first, maxChunkLength)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(buf, streamFirstChunk) {
		return nil, ErrNotSnappy
	}
	return &Reader{r: r, buf: buf}, nil
}

func (z *Reader) Read(p []byte) (int, error) {
Beginning:
	if len(z.remainder) > 0 {
		n := len(z.remainder)
		if len(p) > n {
			copy(p, z.remainder)
			z.remainder = z.remainder[:0]
			return n, nil
		}
		n = len(p)
		copy(p, z.remainder[:n])
		z.remainder = z.remainder[n:]
		return n, nil
	}
	buf := z.buf
	_, err := io.ReadFull(z.r, buf[:4])
	if err != nil {
		return 0, err
	}
	typ := buf[0]
	var length int
	if typ != 0xff {
		length = int(uint32(buf[1]) | uint32(buf[2]<<8) | uint32(buf[3]<<16))
		// length includes the crc, too!
	}

	switch typ {
	case 0xff: // must equal to streamFirstChunk
		if !bytes.Equal(buf, streamFirstChunk[:4]) {
			return 0, ErrNotSnappy
		}
		_, err := io.ReadFull(z.r, buf[:6])
		if err != nil {
			return 0, err
		}
		if !bytes.Equal(buf, streamFirstChunk[4:]) {
			return 0, ErrNotSnappy
		}
		// skip this chunk
		goto Beginning
	case 0x00: // compressed data
		buf = z.buf[:length]
		_, err = io.ReadFull(z.r, buf)
		if err != nil {
			return 0, err
		}
		u := binary.LittleEndian.Uint32(buf[:4])
		if z.remainder, err = snappy.Decode(z.remainder[:cap(z.remainder)], buf[4:]); err != nil {
			return 0, err
		}
		if crc.New(z.remainder).Value() != u {
			return 0, ErrBadChecksum
		}
		goto Beginning
	case 0x01: // uncompressed data
		buf = buf[:4]
		if _, err = io.ReadFull(z.r, buf); err != nil {
			return 0, err
		}
		u := binary.LittleEndian.Uint32(buf)
		n := length - 4
		if len(p) < n {
			n = len(p)
		}
		if n, err = io.ReadFull(z.r, p[:n]); err != nil {
			return n, err
		}
		c := crc.New(p[:n])
		if length > n {
			n2, err := io.ReadFull(z.r, z.remainder[:length-n])
			if err != nil {
				return n + n2, err
			}
			z.remainder = z.remainder[:length-n]
			c.Update(z.remainder)
		}
		if c.Value() != u {
			return n, ErrBadChecksum
		}
		return n, nil
	default:
		if 0x02 <= buf[0] && buf[0] <= 0x7f { // Reserved unskippable chunk
			return 0, ErrUnskippableChunk
		} else if typ == 0xfe || // padding
			0x80 <= typ && typ <= 0xfd { // Reserved skippable chunk
			if _, err = io.CopyN(ioutil.Discard, z.r, int64(length)); err != nil {
				return 0, err
			}
			goto Beginning
		}
	}
	return 0, ErrNotSnappy
}

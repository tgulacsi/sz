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
	crand "crypto/rand"
	"io"
	mrand "math/rand"
	"testing"

	"code.google.com/p/snappy-go/snappy"
	"gopkg.in/inconshreveable/log15.v2"
)

func TestDecompress(t *testing.T) {
	Log.SetHandler(log15.StderrHandler)

	data := make([]byte, 10+8+50)
	for i := 18; i < len(data); i++ {
		data[i] = 1
	}
	copy(data[14:18], crcData(data[18:]))
	copy(data[10:14], []byte("\x01\x36\x00\x00"))

	// test that we check for the initial stream identifier
	d, err := NewReader(bytes.NewReader(data))
	if err == nil || err != ErrNotSnappy {
		t.Errorf("awaited %v, got %v", ErrNotSnappy, err)
	}

	copy(data[:10], streamFirstChunk)
	d, err = NewReader(bytes.NewReader(data))
	if err != nil {
		t.Errorf("NewReader(%q): %v", data, err)
		return
	}
	got := make([]byte, 50)
	n, err := d.Read(got)
	if n != 50 {
		t.Errorf("Read length mismatch: awaited %d, got %d", 50, n)
	}
	if !bytes.Equal(got, data[18:]) {
		t.Errorf("Read data mismatch: awaited\n\t%q,\ngot\n\t%q", data[18:], got)
	}
}

func TestSkip(t *testing.T) {
	Log.SetHandler(log15.StderrHandler)

	// test that we throw error for unskippable chunks
	data := make([]byte, len(streamFirstChunk)+4)
	copy(data, streamFirstChunk)
	copy(data[len(data)-4:], []byte("\x03\x01\x00\x00"))
	d, err := NewReader(bytes.NewReader(data))
	if err != nil {
		t.Errorf("Read: %v", err)
	}

	got := make([]byte, 4)
	if _, err = d.Read(got); err != ErrUnskippableChunk {
		t.Errorf("awaited %v, got %v", ErrUnskippableChunk, err)
	}

	copy(data[len(data)-4:], []byte("\xfe\x01\x00\x00"))
	if d, err = NewReader(bytes.NewReader(data)); err != nil {
		t.Errorf("Read: %v", err)
	}
	if _, err := d.Read(got); err != io.EOF {
		t.Errorf("Read skippable: awaited %v, got %v", io.EOF, err)
	}
}

func TestCheckCRCs(t *testing.T) {
	Log.SetHandler(log15.StderrHandler)

	data := make([]byte, 50)
	for i := 0; i < len(data); i++ {
		data[i] = 1
	}
	compressed, err := snappy.Encode(nil, data)
	buf := make([]byte, len(streamFirstChunk)+8+len(compressed))
	if err != nil {
		t.Fatalf("snappy: %v", err)
	}
	copy(buf[len(buf)-len(compressed):], compressed)
	copy(buf, streamFirstChunk)
	var lengthA [4]byte
	lengthA[1] = byte(len(compressed) + 4)
	copy(buf[len(streamFirstChunk):], lengthA[:])
	realCRC := crcData(data)
	fakeCRC := make([]byte, 4)
	copy(fakeCRC, realCRC)
	fakeCRC[0]++

	copy(buf[len(streamFirstChunk)+4:], fakeCRC)
	d, err := NewReader(bytes.NewReader(buf))
	if err != nil {
		t.Errorf("NewReader: %v", err)
		return
	}
	got := make([]byte, 50)
	t.Logf("bad buf=%q", buf)
	if _, err := d.Read(got); err != ErrBadChecksum {
		t.Errorf("Read: awaited %v, got %v", ErrBadChecksum, err)
	}

	copy(buf[len(streamFirstChunk)+4:], realCRC)
	if d, err = NewReader(bytes.NewReader(buf)); err != nil {
		t.Errorf("NewReader: %v", err)
		return
	}
	t.Logf("good buf=%q", buf)
	n, err := d.Read(got)
	if err != nil {
		t.Errorf("Read: %v", err)
	}
	if n != len(data) {
		t.Errorf("read length mismatch: awaited %d, got %d", len(data), n)
		return
	}
	got = got[:n]
	if !bytes.Equal(data, got) {
		t.Errorf("read byte mismatch: awaited\n\t%q\ngot\n\t%q", data, got)
	}
}

func TestRandom(t *testing.T) {
	Log.SetHandler(log15.StderrHandler)

	randBuf := make([]byte, maxUncomprLength*2)
	var data []byte
	for i := 0; i < 100; i++ {
		var br, bw bytes.Buffer
		c, err := NewWriter(&bw)
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		k := mrand.Intn(3)
		for j := 0; j < k; j++ {
			m := mrand.Intn(cap(randBuf))
			n, err := crand.Read(randBuf[:m])
			if err != nil {
				t.Fatalf("crand: %v", err)
			}
			data = append(data, randBuf[:n]...)
			if n, err = c.Write(randBuf[:n]); err != nil {
				t.Fatalf("Write: %v", err)
			}
		}

		if err = c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		d, err := NewReader(bytes.NewReader(bw.Bytes()))
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		if _, err = io.Copy(&br, d); err != nil {
			t.Fatalf("Copy: %v", err)
		}
		if len(data) != br.Len() {
			t.Errorf("length mismatch: awaited %d, got %d.", len(data), br.Len())
			continue
		}
		if !bytes.Equal(data, br.Bytes()) {
			t.Errorf("data mismatch: awaited %v got %v", data, br.Bytes())
		}
	}
}

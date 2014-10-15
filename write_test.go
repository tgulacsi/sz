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
	"testing"

	lcrc "code.google.com/p/leveldb-go/leveldb/crc"
	"code.google.com/p/snappy-go/snappy"
	"gopkg.in/inconshreveable/log15.v2"
)

func crcData(p []byte) []byte {
	v := lcrc.New(p).Value()
	return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
}

func TestCompression(t *testing.T) {
	Log.SetHandler(log15.StderrHandler)

	var bw bytes.Buffer
	c, err := NewWriter(&bw)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	W := func(data []byte) {
		if _, err := c.Write(data); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := c.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	data := make([]byte, 50, 50+maxUncomprLength*2)
	crc := crcData(data)
	W(data)
	compressed, err := snappy.Encode(make([]byte, 6), data)
	if err != nil {
		t.Fatalf("Encode(%v): %v", data, err)
	}
	got := bw.Bytes()
	awaited := []byte("\xff\x06\x00\x00sNaPpY" +
		"\x00\x0a\x00\x00" + "\x8f)H\xbd" + string(compressed))
	if !bytes.Equal(got, awaited) {
		t.Errorf("compression mismatch: awaited\n\t%q,\ngot\n\t%q", awaited, got)
		return
	}

	n := len(got)
	// add uncompressible chunk
	for i := 0; i < len(data); i++ {
		data[i] = byte(i)
	}
	crc = crcData(data)
	awaited = []byte{116, 22, 50, 22}
	if !bytes.Equal(crc, awaited) {
		t.Errorf("crc mismatch: awaited %v, got %v", crc, awaited)
		return
	}
	W(data)
	got = bw.Bytes()[n:]
	awaited = append(append([]byte("\x00\x36\x00\x00"), crc...), data...)
	if !bytes.Equal(got, awaited) {
		t.Errorf("uncompressible mismatch: awaited\n\t%q,\ngot\n\t%q", awaited, got)
		return
	}

	n += len(got)
	// test that we can add more data than will fit in one chunk
	if _, err = crand.Read(data[n:cap(data)]); err != nil {
		t.Fatalf("crand.Read: %v", err)
	}
	data = data[:cap(data)]
	crc = crcData(data)
	W(data)
	got = bw.Bytes()[n:]
	if len(got) < 131125 {
		t.Errorf("too small: got %d, awaited at least 131125", len(got))
		return
	}
	t.Logf("got %d", len(got))
	// TODO(tgulacsi): check data
}

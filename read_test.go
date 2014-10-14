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

func TestCompression(t *testing.T) {
	Log.SetHandler(log15.StderrHandler)

	var bw bytes.Buffer
	c, err := NewWriter(&bw)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	data := make([]byte, 50)
	if _, err = c.Write(data); err != nil {
		t.Errorf("Write: %v", err)
		return
	}
	if err = c.Flush(); err != nil {
		t.Errorf("Flush: %v", err)
		return
	}
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

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

	"gopkg.in/inconshreveable/log15.v2"
)

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

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

package main

import (
	"bufio"
	"flag"
	"io"
	"os"

	"github.com/tgulacsi/sz"
	"gopkg.in/inconshreveable/log15.v2"
)

var Log = log15.New()

func main() {
	Log.SetHandler(log15.StderrHandler)

	flagDecompress := flag.Bool("d", false, "decompress input")
	flagOut := flag.String("o", "", "output name (default is stdout)")
	flag.Parse()

	fn := flag.Arg(0)
	var inp io.Reader
	if fn == "" || fn == "-" {
		inp = os.Stdin
		defer os.Stdin.Close()
	} else {
		fh, err := os.Open(fn)
		if err != nil {
			Log.Crit("open input", "file", fn, "error", err)
			os.Exit(1)
		}
		defer fh.Close()
		inp = fh
	}

	inp = bufio.NewReader(inp)
	if *flagDecompress {
		r, err := sz.NewReader(inp)
		if err != nil {
			Log.Crit("start reading", "error", err)
			os.Exit(2)
		}
		inp = r
	}

	var out io.WriteCloser
	if *flagOut == "" || *flagOut == "-" {
		out = os.Stdout
	} else {
		fh, err := os.Create(*flagOut)
		if err != nil {
			Log.Crit("create output", "file", *flagOut, "error", err)
			os.Exit(3)
		}
		out = fh
	}
	defer func() {
		if err := out.Close(); err != nil {
			Log.Error("closing output", "file", *flagOut, "error", err)
		}
	}()

	bw := bufio.NewWriter(out)
	defer func() {
		if err := bw.Flush(); err != nil {
			Log.Error("flushing output", "error", err)
		}
	}()
	out = struct {
		io.Writer
		io.Closer
	}{bw, out}
	if !*flagDecompress {
		w, err := sz.NewWriter(out)
		if err != nil {
			Log.Crit("create compressor", "error", err)
			os.Exit(4)
		}
		defer func() {
			if err := w.Close(); err != nil {
				Log.Error("finishing write", "error", err)
			}
		}()
	}

	n, err := io.Copy(out, inp)
	if err != nil {
		Log.Error("copying", "error", err)
	}
	Log.Info("done", "n", n)
	if err = out.Close(); err != nil {
		Log.Error("close", "error", err)
	}
}

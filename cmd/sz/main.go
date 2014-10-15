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
	"time"

	//"github.com/tgulacsi/sz"
	"github.com/mreiferson/go-snappystream"
	"gopkg.in/inconshreveable/log15.v2"
)

var Log = log15.New()

func main() {
	Log.SetHandler(log15.StderrHandler)

	flagVerbose := flag.Bool("v", false, "verbose logging")
	flagDecompress := flag.Bool("d", false, "decompress input")
	flagOut := flag.String("o", "", "output name (default is stdout)")
	flag.Parse()

	if !*flagVerbose {
		Log.SetHandler(log15.LvlFilterHandler(log15.LvlWarn, log15.StderrHandler))
	}

	var err error
	err = do(flag.Arg(0), *flagOut, *flagDecompress)
	if err != nil {
		Log.Error("", "error", err)
		os.Exit(1)
	}
}

func do(inpFn, outFn string, decompress bool) error {
	var inp io.Reader
	if inpFn == "" || inpFn == "-" {
		inp = os.Stdin
		defer os.Stdin.Close()
	} else {
		fh, err := os.Open(inpFn)
		if err != nil {
			return err
		}
		defer fh.Close()
		inp = fh
	}
	inp = bufio.NewReaderSize(inp, 65536)

	var out io.Writer
	if outFn == "" || outFn == "-" {
		out = os.Stdout
		defer os.Stdout.Close()
	} else {
		fh, err := os.Create(outFn)
		if err != nil {
			return err
		}
		out = fh
		defer closeLogErr(fh.Close, "close output")
	}

	bw := bufio.NewWriterSize(out, 65536)
	defer closeLogErr(bw.Flush, "flush output")

	if decompress {
		return doDecompress(inp, out)
	}
	return doCompress(inp, out)
}

func doDecompress(inp io.Reader, out io.Writer) error {
	r := snappystream.NewReader(inp, true)

	t := time.Now()
	n, err := io.Copy(out, r)
	if err != nil {
		Log.Error("copying", "error", err)
		return err
	}
	Log.Info("Finished decompression", "read", n, "time", time.Since(t))
	return nil
}

func doCompress(inp io.Reader, out io.Writer) error {
	w := snappystream.NewBufferedWriter(out)

	t := time.Now()
	n, err := io.Copy(w, inp)
	if err != nil {
		Log.Error("copying", "error", err)
		return err
	}
	Log.Info("Finished compression", "written", n, "time", time.Since(t))
	return w.Close()
}

func closeLogErr(C func() error, message string) func() {
	return func() {
		if err := C(); err != nil {
			Log.Error(message, "error", err)
		}
	}
}

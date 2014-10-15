# sz
This command-line utility implements snappy compression and decompression.
It can be installed by

    go get github.com/tgulacsi/sz

## Usage
Compress from stdin to stdout:

    sz

Compress a file to stdout:

    sz afile

Compress a file to another:

    sz -o bfile afile

Decompress from stdin to stdout:

    sz -d

Decompress a file to stdout:

    sz -d afile

Decompress a file to another:

    sz -d -o bfile afile


## Snappy framing format
See https://code.google.com/p/snappy/source/browse/trunk/framing_format.txt

## Thanks
Thanks for the great github.com/mreiferson/go-snappystream library!
#!/bin/bash
set -euo pipefail

echo 'Building...'
go build -o bin/1brc .

echo 'Running...'
GOGC=off ./bin/1brc >/dev/null # warm cache
GOGC=off ./bin/1brc -cpuprofile default.pgo >/dev/null

echo 'Building with pgo...'
go build -o bin/1brc . # build again with pgo

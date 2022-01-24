#!/usr/bin/env bash

set -xeuo pipefail

docker run --rm -v $(pwd)/test/nft:/project  -w="/project"  truffle --  compile
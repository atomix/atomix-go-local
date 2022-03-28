# SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
#
# SPDX-License-Identifier: Apache-2.0

export CGO_ENABLED=0
export GO111MODULE=on

.PHONY: build

build: # @HELP build the source code
build:
	go build ./...

test: # @HELP run the unit tests and source code validation
test: deps license linters

deps: # @HELP ensure that the required dependencies are in place
	go build -v ./...

linters: # @HELP examines Go source code and reports coding problems
	golangci-lint run

reuse-tool: # @HELP install reuse if not present
	command -v reuse || python3 -m pip install reuse

license: reuse-tool # @HELP run license checks
	reuse lint

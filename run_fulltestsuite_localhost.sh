#!/bin/bash
export SERVER_ENDPOINT=localhost:9000
export ACCESS_KEY=minioadmin
export SECRET_KEY=minioadmin
export ENABLE_HTTPS=true
export SKIP_INSECURE=true
export SKIP_BUILD=false
export ALIAS="minio"

go test -v ./... -run Test_FullSuite

#!/usr/bin/env bash

protoDir="pb"
outDir="pb"
protoc -I ${protoDir}/  ${protoDir}/pb.proto --gofast_out=plugins=grpc:${outDir}


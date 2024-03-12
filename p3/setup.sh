#!/bin/bash
# This is for AUTOGRADING ONLY
pushd submission
cp -R ../{workload,{autograde,../tester}.py} .
python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. mathdb.proto
popd

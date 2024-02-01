#!/bin/bash
protoc -I protocol_definition/ --python_out src/omotes_orchestrator/proto_gen/ ./protocol_definition/omotes.proto
protoc -I protocol_definition/ --mypy_out src/omotes_orchestrator/proto_gen/ ./protocol_definition/omotes.proto

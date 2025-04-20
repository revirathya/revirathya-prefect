#!/bin/bash

FLOW_PATH=$1

# Build Image
docker build -t avidito/revirathya-prefect:3-0.1.0 .

# Deploy Flow
uv run -m flows.$FLOW_PATH
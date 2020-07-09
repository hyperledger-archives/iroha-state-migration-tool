#!/bin/bash -ex

# cd to project root
cd "$(dirname $0)/../.."

VERSION=1.2.0
TAG=soramitsu/iroha-state-migration-tool:$VERSION

docker build -t $TAG .
docker push $TAG
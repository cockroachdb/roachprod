#!/usr/bin/env bash

echo "${TEST_CLUSTER_SSH_KEY}" > id_test_cluster.rsa
mkdir -p artifacts
docker run \
    --workdir=/go/src/github.com/cockroachdb/roachprod \
    --volume="${GOPATH%%:*}/src":/go/src \
    --volume="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)":/go/src/github.com/cockroachdb/roachprod \
    --env="AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
    --env="AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
    --env="GOOGLE_CREDENTIALS=${GOOGLE_CREDENTIALS}" \
    --rm \
    cockroachdb/builder:20171004-085709 ci/nightly.sh

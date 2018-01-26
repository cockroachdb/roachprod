#!/usr/bin/env bash

go get ./...
go install

curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-182.0.0-linux-x86_64.tar.gz
tar -zxf google-cloud-sdk-182.0.0-linux-x86_64.tar.gz
./google-cloud-sdk/install.sh
source google-cloud-sdk/path.bash.inc

echo $GOOGLE_CREDENTIALS > creds.json

gcloud auth activate-service-account --key-file=creds.json

# It might already exist.
roachprod -u teamcity create teamcity-nightly || roachprod sync

eval $(ssh-agent)
ssh-add ~/.ssh/google_compute_engine

curl -L https://edge-binaries.cockroachdb.com/cockroach/cockroach.linux-gnu-amd64.LATEST -o cockroach
chmod +x cockroach
curl -L https://edge-binaries.cockroachdb.com/loadgen/kv.LATEST -o kv
chmod +x kv

roachprod put teamcity-nightly ./cockroach ./cockroach
roachprod put teamcity-nightly ./kv ./kv

cd artifacts
roachprod test teamcity-nightly nightly
roachprod upload $(ls)

roachprod -u teamcity destroy teamcity-nightly

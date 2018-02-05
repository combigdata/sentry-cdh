#!/usr/bin/env bash
DEFAULT_BRANCH=cdh6.x
: ${BUILD_BRANCH:=${DEFAULT_BRANCH}}
set +e

## get the GBN of the current BUILD_BRANCH
pattern='^([a-zA-Z]+)([^_]+)(_*(\S+))*$'
[[ "${BUILD_BRANCH}" =~ $pattern ]]
PRODUCT=${BASH_REMATCH[1]}
VERSION=${BASH_REMATCH[2]}
TAG=${BASH_REMATCH[4]}
: ${TAG:=official}


export CDH_GBN=$(curl --silent --show-error --fail "http://builddb.infra.cloudera.com:8080/query?product=cdh&user=jenkins&version=${VERSION}&tag=${TAG}")
if [ $? -ne 0 ]
then
  ## no GBN for BUILD_BRANCH then use the 6.x lastest
  echo "############# WARNING: Unable to find latest GBN for ${BUILD_BRANCH}. Using cdh6.x official by default. ##############"
  export CDH_GBN=$(curl --silent --show-error --fail 'http://builddb.infra.cloudera.com:8080/query?product=cdh&user=jenkins&version=6.x&tag=official')
fi
set -e

### get the C6 GBN based pom
: ${TMPDIR:=/tmp/}
BUILD_SCRATCH_DIR=$(mktemp -td "$(basename $0).XXXXXXXXXXXX")

function finish {
  rm -rf "${BUILD_SCRATCH_DIR}"
}

trap finish EXIT

export BUILD_SETTINGS_FILE="${BUILD_SCRATCH_DIR}/gbn-m2-settings.xml"

curl --silent --show-error --fail "http://github.mtv.cloudera.com/raw/CDH/cdh/cdh6.x/gbn-m2-settings.xml" -o "${BUILD_SETTINGS_FILE}"

# replacing the environment variable is not strictly necessary but it can useful
# if the pom is encapsulated
sed "s/\\\${env.CDH_GBN}/$CDH_GBN/g" -i "${BUILD_SETTINGS_FILE}"



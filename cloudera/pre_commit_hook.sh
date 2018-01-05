#!/usr/bin/env bash
echo "Running tests using JDK8"
set -x
set -e

export JAVA8_BUILD=true
#source /opt/toolchain/toolchain.sh


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# get the CDH_GBN
export BUILD_BRANCH=${GERRIT_BRANCH}
source $DIR/setup_build_env.sh

# clean the old test class compiles
find . -name test-classes | grep target/test-classes | xargs rm -rf

# execute all tests (disable slow tests for now)
mvn -s "${BUILD_SETTINGS_FILE}" test -PskipOnlySlowTests --fail-at-end

#!/usr/bin/env bash
echo "Running tests using JDK8"
set -x
set -e

export JAVA8_BUILD=true
source /opt/toolchain/toolchain.sh


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# get the CDH_GBN
export BUILD_BRANCH=${GERRIT_BRANCH}
source $DIR/setup_build_env.sh

# clean the old test class compiles
find . -name test-classes | grep target/test-classes | xargs rm -rf

# For now, just verify the code compiles.
mvn -s "${BUILD_SETTINGS_FILE}" clean compile package -DskipTests -Dmaven.test.failure.ignore=true

# Run a subset of tests to quickly verify patches
mvn -s "${BUILD_SETTINGS_FILE}" test -PskipSlowAndNotThreadSafeTests --fail-at-end

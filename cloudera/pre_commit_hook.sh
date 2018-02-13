#!/usr/bin/env bash
echo "Running tests using JDK8"
set -x
set -e

export JAVA8_BUILD=true
source /opt/toolchain/toolchain.sh


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# activate mvn-gbn wrapper
mv "$(which mvn-gbn-wrapper)" "$(dirname "$(which mvn-gbn-wrapper)")/mvn"

# clean the old test class compiles
find . -name test-classes | grep target/test-classes | xargs rm -rf

# execute all tests (disable slow tests for now)
mvn test -PskipOnlySlowTests --fail-at-end

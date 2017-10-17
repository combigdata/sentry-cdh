#!/usr/bin/env bash
echo "Running tests using JDK8"

set -x
export JAVA8_BUILD=true
source /opt/toolchain/toolchain.sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# get the CDH_GBN
export BUILD_BRANCH=${GERRIT_BRANCH}
source $DIR/setup_build_env.sh

find . -name test-classes | grep target/test-classes | xargs rm -rf

mvn  -s "${BUILD_SETTINGS_FILE}" clean compile package -DskipTests=true

# enable full tests for now
mvn  -s "${BUILD_SETTINGS_FILE}" test -PskipSlowAndNotThreadSafeTests --fail-fast -DtestFailureIgnore=false

# validate test status and email failures to author
res=$?
if [[ "${res}" -eq 0 ]]; then
  exit 0
fi
if [[ -z "${BUILD_URL}" ]] || [[ "${BUILD_URL}xxx" = "xxx" ]] ; then
  BUILD_URL="https://master-01.jenkins.cloudera.com/job/cdh6.x-sentry-Unit/lastBuild/"
fi
echo "Please check out details here: ${BUILD_URL}" > /tmp/sentry-unit-tests.log
EML="sentry-jenkins@cloudera.com"
if [[ ! -z "${GIT_BRANCH}" ]] && [[ "${GIT_BRANCH}xxx" != "xxx" ]] ; then
  EML=$(git shortlog ${GIT_BRANCH} -1 -se | awk -F"<" '{print $2}' | awk -F">" '{print $1}')
fi
sudo mkdir -p /etc/ssmtp
sudo echo "root=postmaster" > /etc/ssmtp/ssmtp.conf
sudo echo "mailhub=cloudera.com:25" >> /etc/ssmtp/ssmtp.conf
mail -s "Most Recent Sentry Post Commit Job Failed" ${EML} < /tmp/sentry-unit-tests.log
exit ${res}

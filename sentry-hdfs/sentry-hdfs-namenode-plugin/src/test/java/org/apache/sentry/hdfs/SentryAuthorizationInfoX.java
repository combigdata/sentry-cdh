/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.hdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.Lists;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.hdfs.service.thrift.TPrivilegePrincipal;
import org.apache.sentry.hdfs.service.thrift.TPrivilegePrincipalType;
import org.apache.sentry.hdfs.service.thrift.TRoleChanges;
import org.apache.hadoop.conf.Configuration;


public class SentryAuthorizationInfoX extends SentryAuthorizationInfo {
  static Map<String, String> hiveObjPathMapping = new HashMap<>();
  static List<String> hiveObjPaths = new ArrayList<>();

  static String hdfsPathPrefixes;
  static final String ROLE = "test-role";
  static final String GROUP = "test-group";

  public SentryAuthorizationInfoX(Configuration conf) throws Exception {
    super(conf);
    System.setProperty("test.stale", "false");

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    int seqNumber = 1;
    PathsUpdate pathUpdate;
    TPathChanges pathChange;
    PermissionsUpdate permUpdate;
    String path;
    for (Map.Entry<String, String> entry : hiveObjPathMapping.entrySet()) {
      // Add a new path
      pathUpdate = new PathsUpdate(seqNumber, false);
      pathChange = pathUpdate.newPathChange(entry.getKey());
      path = PathsUpdate.parsePath(entry.getValue());
      List<String> paths = Lists.newArrayList(path.split("/"));
      pathChange.addToAddPaths(paths);
      getAuthzPaths().updatePartial(Lists.newArrayList(pathUpdate), lock);
      // Add a permission
      permUpdate = new PermissionsUpdate(seqNumber, false);
      permUpdate.addPrivilegeUpdate(entry.getKey()).putToAddPrivileges(
          new TPrivilegePrincipal(TPrivilegePrincipalType.ROLE, ROLE), "*");
      getAuthzPermissions().updatePartial(Lists.newArrayList(permUpdate), lock);
      seqNumber++;
    }
    // Generate the permission add update for role "test-groups"
    permUpdate = new PermissionsUpdate(seqNumber, false);
    TRoleChanges addrUpdate = permUpdate.addRoleUpdate(ROLE);
    addrUpdate.addToAddGroups(GROUP);
    getAuthzPermissions().updatePartial(Lists.newArrayList(permUpdate), lock);
  }

  @Override
  public void run() {

  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public boolean isStale() {
    String stale = System.getProperty("test.stale");
    return stale.equalsIgnoreCase("true");
  }

  static void initializeTestData() {
    hiveObjPathMapping.put("db1.tbl12", "hdfs:///user/authz/db1/tbl12");
    hiveObjPathMapping.put("db1.tbl13", "hdfs:///user/authz/db1/tbl13");
    hiveObjPathMapping.put("db1.tbl14", "hdfs:///user/authz/db1/tbl14/part121");
    hiveObjPaths.add("/user/authz/db1/tbl12");
    hiveObjPaths.add("/user/authz/db1/tbl13");
    hiveObjPaths.add("/user/authz/db1/tbl14/part121");
  }

}

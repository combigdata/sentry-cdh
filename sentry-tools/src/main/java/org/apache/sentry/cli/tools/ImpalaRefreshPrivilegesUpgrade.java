/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.cli.tools;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.sentry.api.common.ApiConstants.PrivilegeScope;
import org.apache.sentry.api.service.thrift.TSentryGrantOption;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.persistent.SentryStoreInterface;

/**
 * Utility class to grant the REFRESH privilege to those roles that are part of the Sentry upgrade
 * in CDH 5.16, CDH 6.1 and higher.
 *
 * <p/>Those roles with a SELECT or INSERT privilege that were granted before the upgrade will be candidates
 * to have the REFRESH privilege during the upgrade.
 */
public class ImpalaRefreshPrivilegesUpgrade implements SentryStoreUpgrade {
  private static final String REFRESH = "REFRESH";
  private static final String SELECT = "SELECT";
  private static final String INSERT = "INSERT";

  private final Set<String> ALLOWED_SCOPES = Sets.newHashSet(
    PrivilegeScope.SERVER.toString(),
    PrivilegeScope.DATABASE.toString(),
    PrivilegeScope.TABLE.toString()
  );

  private final Set<String> UPGRADES_FROM_VERSIONS = Sets.newHashSet(
    // CDH 5.x schema versions previous to CDH 5.16
    "1.4.0",
    "1.4.0-cdh5",
    "1.4.0-cdh5-2",
    "1.5.0",
    "1.5.0-cdh5",

    // CDH 6.0
    "2.0.0"
  );

  @Override
  public boolean needsUpgrade(String fromVersion) {
    return UPGRADES_FROM_VERSIONS.contains(fromVersion.toLowerCase());
  }

  /**
   * This class is used to store only the required fields of a privilege to implement
   * the hashCode and equals methods required by the Set object.
   */
  private static class RefreshPrivilege {
    static RefreshPrivilege copyFrom(TSentryPrivilege from) {
      return new RefreshPrivilege(from.getPrivilegeScope(), from.getServerName(), from.getDbName(), from.getTableName());
    }

    private String scope, server, db, table;

    private RefreshPrivilege(String scope, String server, String db, String table) {
      this.scope = scope;
      this.server = server;
      this.db = db;
      this.table = table;
    }

    TSentryPrivilege toSentryPrivilege() {
      TSentryPrivilege p = new TSentryPrivilege();
      p.setPrivilegeScope(scope);
      p.setServerName(server);
      p.setAction(REFRESH);

      if (db != null) {
        p.setDbName(db);
      }

      if (table != null) {
        p.setTableName(table);
      }

      // Do not give grant option to the refresh privilege
      p.setGrantOption(TSentryGrantOption.FALSE);
      return p;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(scope, server, db, table);
    }

    @Override
    public boolean equals(Object o) {
      if (o == null) {
        return false;
      }

      if (!(o instanceof RefreshPrivilege)) {
        return false;
      }

      RefreshPrivilege op = (RefreshPrivilege)o;
      return (StringUtils.equals(scope, op.scope)
        && StringUtils.equals(server, op.server)
        && StringUtils.equals(db, op.db)
        && StringUtils.equals(table, op.table));
    }
  }

  /**
   * Grant (or insert) new REFRESH privileges to all roles that have the SELECT and/or INSERT privileges.
   *
   * @param store The SentryStore object used to grant the new privileges.
   * @param userName The user who is executing this upgrade (usually the admin user).
   */
  @Override
  public void upgrade(String userName, SentryStoreInterface store) throws Exception {
    Map<String, Set<TSentryPrivilege>> rolesPrivileges = store.getAllRolesPrivileges();
    for (String roleName : rolesPrivileges.keySet()) {
      Set<RefreshPrivilege> refreshPrivilegesToAdd = Sets.newHashSet();
      for (TSentryPrivilege p : rolesPrivileges.get(roleName)) {
        // Add a new REFRESH privilege in the presence of a SELECT or INSERT.
        // Using the Set and the RefreshPrivilege object allows us to have unique REFRESH privileges
        // in case SELECT and INSERT exist in the same authorization scope.
        if ((checkPrivilege(p, SELECT) || checkPrivilege(p, INSERT)) && isScopeAllowed(p)) {
          refreshPrivilegesToAdd.add(RefreshPrivilege.copyFrom(p));
        } else if (checkPrivilege(p, REFRESH)) {
          // If a REFRESH privilege already exists, then attempt to remove it from the Set
          // (if it was added before in the presence of a SELECT or INSERT) to avoid duplicating
          // REFRESH privileges in the same scope.
          refreshPrivilegesToAdd.remove(RefreshPrivilege.copyFrom(p));
        }
      }

      if (!refreshPrivilegesToAdd.isEmpty()) {
        for (RefreshPrivilege p : refreshPrivilegesToAdd) {
          store.alterSentryRoleGrantPrivileges(userName, roleName,
            Collections.singleton(p.toSentryPrivilege()));
        }
      }
    }
  }

  private boolean checkPrivilege(TSentryPrivilege p, String action) {
    return (p != null && p.getAction().equalsIgnoreCase(action));
  }

  private boolean isScopeAllowed(TSentryPrivilege p) {
    return (p != null && ALLOWED_SCOPES.contains(p.getPrivilegeScope().toUpperCase()));
  }
}

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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.sentry.api.common.ApiConstants.PrivilegeScope;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.service.common.ServiceConstants.SentryPrincipalType;
import org.junit.Test;
import org.mockito.Mockito;

public class TestImpalaRefreshPrivilegesUpgrade {
  private static final String CDH5_2 = "1.4.0";
  private static final String CDH5_4 = "1.4.0-cdh5-2";
  private static final String CDH5_12 = "1.5.0";
  private static final String CDH5_15 = "1.5.0-cdh5";
  private static final String CDH5_16 = "1.5.0-cdh5-2";
  private static final String CDH5_17 = "1.5.0-cdh5-3"; // hypothetical schema version
  private static final String CDH6_0 = "2.0.0";
  private static final String CDH6_1 = "2.1.0";
  private static final String CDH6_2 = "2.2.0"; // hypothetical schema version

  private static final String REFRESH = "REFRESH";

  @Test
  public void testValidUpgradesScenarios() {
    SentryStoreUpgrade storeUpgrade = new ImpalaRefreshPrivilegesUpgrade();

    assertTrue("Upgrade from CDH 5.2 must be allowed", storeUpgrade.needsUpgrade(CDH5_2));
    assertTrue("Upgrade from CDH 5.4 must be allowed", storeUpgrade.needsUpgrade(CDH5_4));
    assertTrue("Upgrade from CDH 5.12 must be allowed",storeUpgrade.needsUpgrade(CDH5_12));
    assertTrue("Upgrade from CDH 5.15 must be allowed",storeUpgrade.needsUpgrade(CDH5_15));
    assertTrue("Upgrade from CDH 6.0 must be allowed",storeUpgrade.needsUpgrade(CDH6_0));
  }

  @Test
  public void testInvalidUpgradesScenarios() {
    SentryStoreUpgrade storeUpgrade = new ImpalaRefreshPrivilegesUpgrade();

    assertFalse(storeUpgrade.needsUpgrade(CDH5_16));
    assertFalse(storeUpgrade.needsUpgrade(CDH5_17));
    assertFalse(storeUpgrade.needsUpgrade(CDH6_1));
    assertFalse(storeUpgrade.needsUpgrade(CDH6_2));
  }

  @Test
  public void testIgnoreRefreshPrivilegesWhenAllIsFound() throws Exception {
    SentryStoreUpgrade storeUpgrade = new ImpalaRefreshPrivilegesUpgrade();
    SentryStore mockStore = Mockito.mock(SentryStore.class);

    Set<TSentryPrivilege> ALL_PRIVILEGES_AND_SCOPES = Sets.newHashSet(
      toTSentryPrivilege(PrivilegeScope.SERVER, AccessConstants.ACTION_ALL),
      toTSentryPrivilege(PrivilegeScope.DATABASE, AccessConstants.ACTION_ALL),
      toTSentryPrivilege(PrivilegeScope.TABLE, AccessConstants.ACTION_ALL),
      toTSentryPrivilege(PrivilegeScope.COLUMN, AccessConstants.ACTION_ALL),
      toTSentryPrivilege(PrivilegeScope.URI, AccessConstants.ACTION_ALL),
      toTSentryPrivilege(PrivilegeScope.SERVER, AccessConstants.ALL),
      toTSentryPrivilege(PrivilegeScope.DATABASE, AccessConstants.ALL),
      toTSentryPrivilege(PrivilegeScope.TABLE, AccessConstants.ALL),
      toTSentryPrivilege(PrivilegeScope.COLUMN, AccessConstants.ALL),
      toTSentryPrivilege(PrivilegeScope.URI, AccessConstants.ALL)
    );

    Map<String, Set<TSentryPrivilege>> inputRolesPrivileges = new HashMap<>();
    inputRolesPrivileges.put("role1", ALL_PRIVILEGES_AND_SCOPES);
    inputRolesPrivileges.put("role2", ALL_PRIVILEGES_AND_SCOPES);

    Mockito.when(mockStore.getAllRolesPrivileges()).thenReturn(inputRolesPrivileges);

    storeUpgrade.upgrade(mockStore);
    Mockito.verify(mockStore, Mockito.times(1)).getAllRolesPrivileges();
    Mockito.verifyZeroInteractions(mockStore);
  }

  @Test
  public void testAddRefreshPrivilegesWhenSelectOrInsertAreFound() throws Exception {
    SentryStoreUpgrade storeUpgrade = new ImpalaRefreshPrivilegesUpgrade();
    SentryStore mockStore = Mockito.mock(SentryStore.class);

    Set<TSentryPrivilege> ALL_PRIVILEGES_AND_SCOPES = Sets.newHashSet(
      toTSentryPrivilege(PrivilegeScope.SERVER, AccessConstants.SELECT),
      toTSentryPrivilege(PrivilegeScope.SERVER, AccessConstants.INSERT),
      toTSentryPrivilege(PrivilegeScope.DATABASE, AccessConstants.SELECT),
      toTSentryPrivilege(PrivilegeScope.DATABASE, AccessConstants.INSERT),
      toTSentryPrivilege(PrivilegeScope.TABLE, AccessConstants.SELECT),
      toTSentryPrivilege(PrivilegeScope.TABLE, AccessConstants.INSERT),
      toTSentryPrivilege(PrivilegeScope.COLUMN, AccessConstants.SELECT),
      toTSentryPrivilege(PrivilegeScope.COLUMN, AccessConstants.INSERT),
      toTSentryPrivilege(PrivilegeScope.URI, AccessConstants.SELECT)
    );

    Map<String, Set<TSentryPrivilege>> inputRolesPrivileges = new HashMap<>();
    inputRolesPrivileges.put("role1", ALL_PRIVILEGES_AND_SCOPES);
    inputRolesPrivileges.put("role2", ALL_PRIVILEGES_AND_SCOPES);

    Mockito.when(mockStore.getAllRolesPrivileges()).thenReturn(inputRolesPrivileges);

    storeUpgrade.upgrade(mockStore);

    Set<TSentryPrivilege> NEW_REFRESH_PRIVILEGES_AND_SCOPES = Sets.newHashSet(
      toTSentryPrivilege(PrivilegeScope.SERVER, REFRESH),
      toTSentryPrivilege(PrivilegeScope.DATABASE, REFRESH),
      toTSentryPrivilege(PrivilegeScope.TABLE, REFRESH)
    );

    for (TSentryPrivilege grantedPrivileges : NEW_REFRESH_PRIVILEGES_AND_SCOPES) {
      Mockito.verify(mockStore).alterSentryGrantPrivilege(
        SentryPrincipalType.ROLE, "role1", grantedPrivileges);
      Mockito.verify(mockStore).alterSentryGrantPrivilege(
        SentryPrincipalType.ROLE, "role2", grantedPrivileges);
    }
  }

  private TSentryPrivilege toTSentryPrivilege(PrivilegeScope scope, String action) {
    TSentryPrivilege privilege = new TSentryPrivilege();

    privilege.setPrivilegeScope(scope.toString());
    privilege.setServerName("server1");

    if (scope != PrivilegeScope.SERVER) {
      privilege.setDbName("db1");
    }

    if (scope != PrivilegeScope.SERVER && scope != PrivilegeScope.DATABASE) {
      privilege.setTableName("tbl1");
    }

    privilege.setAction(action);
    return privilege;
  }
}

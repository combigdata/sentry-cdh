/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.service.thrift;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.sentry.hdfs.PermissionsUpdate;
import org.apache.sentry.hdfs.Updateable.Update;
import org.apache.sentry.hdfs.service.thrift.TPrivilegePrincipal;
import org.apache.sentry.hdfs.service.thrift.TPrivilegePrincipalType;
import org.apache.sentry.provider.db.audit.SentryAuditLogger;
import org.apache.sentry.provider.db.service.persistent.SentryStoreInterface;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryPrincipalType;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.service.thrift.ServiceConstants.PrivilegeScope;
import org.apache.sentry.service.thrift.ServiceConstants.SentryPrincipalType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.USER;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.GROUP;
import static org.apache.sentry.core.model.db.AccessConstants.OWNER;
import static org.junit.Assert.assertEquals;

public class TestSentryHMSOwnerHandler {
  private static final String SENTRY_SERVICE_USER = "sentry";
  private static final String AUTHZ_SERVER_NAME = "server1";
  private static final boolean WITH_GRANT = true;
  private static final boolean NO_GRANT = false;
  private static final String NO_TABLE = null;
  private static final String MANAGED_TABLE = TableType.MANAGED_TABLE.name();
  private static final String VIRTUAL_VIEW = TableType.VIRTUAL_VIEW.name();

  private static final Map<PrincipalType, TPrivilegePrincipalType> SENTRY_T_PRIVILEGE_PRINCIPAL_TYPE_MAP =
    ImmutableMap.of(
      ROLE, TPrivilegePrincipalType.ROLE,
      USER, TPrivilegePrincipalType.USER
    );

  private static final Map<PrincipalType, TSentryPrincipalType> SENTRY_T_PRINCIPAL_TYPE_MAP =
    ImmutableMap.of(
      ROLE, TSentryPrincipalType.ROLE,
      USER, TSentryPrincipalType.USER
    );

  private static final Map<PrincipalType, SentryPrincipalType> SENTRY_PRINCIPAL_TYPE_MAP =
    ImmutableMap.of(
      ROLE, SentryPrincipalType.ROLE,
      USER, SentryPrincipalType.USER
    );

  private SentryHMSOwnerHandler ownershipHandler;
  private SentryStoreInterface mockStore;
  private SentryAuditLogger mockAudit;

  @Before
  public void classSetup() {
    // Mock objects
    mockStore = Mockito.mock(SentryStoreInterface.class);
    mockAudit = Mockito.mock(SentryAuditLogger.class);

    ownershipHandler = new SentryHMSOwnerHandler(mockStore, mockAudit,
      AUTHZ_SERVER_NAME, SENTRY_SERVICE_USER);
  }

  @Test
  public void testGrantOwnerOnDatabaseToUser() throws Exception {
    // Grant owner privileges to user without grant option
    ownershipHandler.grantDatabaseOwnerPrivilege("db1", USER, "u1", NO_GRANT);
    verifyGrantOwnerPrivilegeStore("db1", MANAGED_TABLE, NO_TABLE, USER, "u1", NO_GRANT);
    verifyGrantOwnerPrivilegeAudit(SENTRY_SERVICE_USER, "db1", NO_TABLE, USER, "u1");
    
    // Drop last grant privileges
    stubListOwnersByAuthorizable("db1", NO_TABLE, USER, "u1");
    ownershipHandler.dropDatabaseOwnerPrivileges("db1");
    verifyRevokeOwnerPrivilegeStore("db1", MANAGED_TABLE, NO_TABLE, USER, "u1");

    // Grant owner privileges to user with grant option
    ownershipHandler.grantDatabaseOwnerPrivilege("db1", USER, "u1", WITH_GRANT);
    verifyGrantOwnerPrivilegeStore("db1", MANAGED_TABLE, NO_TABLE, USER, "u1", WITH_GRANT);
    verifyGrantOwnerPrivilegeAudit(SENTRY_SERVICE_USER, "db1", NO_TABLE, USER, "u1");
    
    // Drop last grant privileges
    stubListOwnersByAuthorizable("db1", NO_TABLE, USER, "u1");
    ownershipHandler.dropDatabaseOwnerPrivileges("db1");
    verifyRevokeOwnerPrivilegeStore("db1", MANAGED_TABLE, NO_TABLE, USER, "u1");
  }

  @Test
  public void testGrantOwnerOnDatabaseToRole() throws Exception {
    // Grant owner privileges to role without grant option
    ownershipHandler.grantDatabaseOwnerPrivilege("db1", ROLE, "r1", NO_GRANT);
    verifyGrantOwnerPrivilegeStore("db1", MANAGED_TABLE, NO_TABLE, ROLE, "r1", NO_GRANT);
    verifyGrantOwnerPrivilegeAudit(SENTRY_SERVICE_USER, "db1", NO_TABLE, ROLE, "r1");

    // Drop last grant privileges
    stubListOwnersByAuthorizable("db1", NO_TABLE, ROLE, "r1");
    ownershipHandler.dropDatabaseOwnerPrivileges("db1");
    verifyRevokeOwnerPrivilegeStore("db1", MANAGED_TABLE, NO_TABLE, ROLE, "r1");

    // Grant owner privileges to role with grant option
    ownershipHandler.grantDatabaseOwnerPrivilege("db1", ROLE, "r1", WITH_GRANT);
    verifyGrantOwnerPrivilegeStore("db1", MANAGED_TABLE, NO_TABLE, ROLE, "r1", WITH_GRANT);
    verifyGrantOwnerPrivilegeAudit(SENTRY_SERVICE_USER, "db1", NO_TABLE, ROLE, "r1");

    // Drop last grant privileges
    stubListOwnersByAuthorizable("db1", NO_TABLE, ROLE, "r1");
    ownershipHandler.dropDatabaseOwnerPrivileges("db1");
    verifyRevokeOwnerPrivilegeStore("db1", MANAGED_TABLE, NO_TABLE, ROLE, "r1");
  }

  @Test
  public void testGrantOwnerOnTableToUser() throws Exception {
    // Grant owner privileges to user without grant option
    ownershipHandler.grantTableOwnerPrivilege("db1", MANAGED_TABLE, "t1", USER, "u1", NO_GRANT);
    verifyGrantOwnerPrivilegeStore("db1", MANAGED_TABLE, "t1", USER, "u1", NO_GRANT);
    verifyGrantOwnerPrivilegeAudit(SENTRY_SERVICE_USER, "db1", "t1", USER, "u1");

    // Drop last grant privileges
    stubListOwnersByAuthorizable("db1", "t1", USER, "u1");
    ownershipHandler.dropTableOwnerPrivileges("db1", MANAGED_TABLE, "t1");
    verifyRevokeOwnerPrivilegeStore("db1", MANAGED_TABLE, "t1", USER, "u1");

    // Grant owner privileges to user with grant option
    ownershipHandler.grantTableOwnerPrivilege("db1", MANAGED_TABLE, "t1", USER, "u1", WITH_GRANT);
    verifyGrantOwnerPrivilegeStore("db1", MANAGED_TABLE, "t1", USER, "u1", WITH_GRANT);
    verifyGrantOwnerPrivilegeAudit(SENTRY_SERVICE_USER, "db1", "t1", USER, "u1");

    // Drop last grant privileges
    stubListOwnersByAuthorizable("db1", "t1", USER, "u1");
    ownershipHandler.dropTableOwnerPrivileges("db1", MANAGED_TABLE, "t1");
    verifyRevokeOwnerPrivilegeStore("db1", MANAGED_TABLE, "t1", USER, "u1");
  }

  @Test
  public void testGrantOwnerOnTableToRole() throws Exception {
    // Grant owner privileges to user without grant option
    ownershipHandler.grantTableOwnerPrivilege("db1", MANAGED_TABLE, "t1", ROLE, "r1", NO_GRANT);
    verifyGrantOwnerPrivilegeStore("db1", MANAGED_TABLE, "t1", ROLE, "r1", NO_GRANT);
    verifyGrantOwnerPrivilegeAudit(SENTRY_SERVICE_USER, "db1", "t1", ROLE, "r1");

    // Drop last grant privileges
    stubListOwnersByAuthorizable("db1", "t1", ROLE, "r1");
    ownershipHandler.dropTableOwnerPrivileges("db1", MANAGED_TABLE, "t1");
    verifyRevokeOwnerPrivilegeStore("db1", MANAGED_TABLE, "t1", ROLE, "r1");

    // Grant owner privileges to user with grant option
    ownershipHandler.grantTableOwnerPrivilege("db1", MANAGED_TABLE, "t1", ROLE, "r1", WITH_GRANT);
    verifyGrantOwnerPrivilegeStore("db1", MANAGED_TABLE, "t1", ROLE, "r1", WITH_GRANT);
    verifyGrantOwnerPrivilegeAudit(SENTRY_SERVICE_USER, "db1", "t1", ROLE, "r1");

    // Drop last grant privileges
    stubListOwnersByAuthorizable("db1", "t1", ROLE, "r1");
    ownershipHandler.dropTableOwnerPrivileges("db1", MANAGED_TABLE, "t1");
    verifyRevokeOwnerPrivilegeStore("db1", MANAGED_TABLE, "t1", ROLE, "r1");
  }

  @Test
  public void testGrantOwnerOnViewToUser() throws Exception {
    // Grant owner privileges to user without grant option
    ownershipHandler.grantTableOwnerPrivilege("db1", VIRTUAL_VIEW, "v1", USER, "u1", NO_GRANT);
    verifyGrantOwnerPrivilegeStore("db1", VIRTUAL_VIEW, "v1", USER, "u1", NO_GRANT);
    verifyGrantOwnerPrivilegeAudit(SENTRY_SERVICE_USER, "db1", "v1", USER, "u1");

    // Drop last grant privileges
    stubListOwnersByAuthorizable("db1", "v1", USER, "u1");
    ownershipHandler.dropTableOwnerPrivileges("db1", VIRTUAL_VIEW, "v1");
    verifyRevokeOwnerPrivilegeStore("db1", VIRTUAL_VIEW, "v1", USER, "u1");

    // Grant owner privileges to user with grant option
    ownershipHandler.grantTableOwnerPrivilege("db1", VIRTUAL_VIEW, "v1", USER, "u1", WITH_GRANT);
    verifyGrantOwnerPrivilegeStore("db1", VIRTUAL_VIEW, "v1", USER, "u1", WITH_GRANT);
    verifyGrantOwnerPrivilegeAudit(SENTRY_SERVICE_USER, "db1", "v1", USER, "u1");

    // Drop last grant privileges
    stubListOwnersByAuthorizable("db1", "v1", USER, "u1");
    ownershipHandler.dropTableOwnerPrivileges("db1", VIRTUAL_VIEW, "v1");
    verifyRevokeOwnerPrivilegeStore("db1", VIRTUAL_VIEW, "v1", USER, "u1");
  }

  @Test
  public void testGrantOwnerOnViewToRole() throws Exception {
    // Grant owner privileges to user without grant option
    ownershipHandler.grantTableOwnerPrivilege("db1", VIRTUAL_VIEW, "v1", ROLE, "r1", NO_GRANT);
    verifyGrantOwnerPrivilegeStore("db1", VIRTUAL_VIEW, "v1", ROLE, "r1", NO_GRANT);
    verifyGrantOwnerPrivilegeAudit(SENTRY_SERVICE_USER, "db1", "v1", ROLE, "r1");

    // Drop last grant privileges
    stubListOwnersByAuthorizable("db1", "t1", ROLE, "r1");
    ownershipHandler.dropTableOwnerPrivileges("db1", VIRTUAL_VIEW, "v1");
    verifyRevokeOwnerPrivilegeStore("db1", VIRTUAL_VIEW, "v1", ROLE, "r1");

    // Grant owner privileges to user with grant option
    ownershipHandler.grantTableOwnerPrivilege("db1", VIRTUAL_VIEW, "v1", ROLE, "r1", WITH_GRANT);
    verifyGrantOwnerPrivilegeStore("db1", VIRTUAL_VIEW, "v1", ROLE, "r1", WITH_GRANT);
    verifyGrantOwnerPrivilegeAudit(SENTRY_SERVICE_USER, "db1", "v1", ROLE, "r1");

    // Drop last grant privileges
    stubListOwnersByAuthorizable("db1", "v1", ROLE, "r1");
    ownershipHandler.dropTableOwnerPrivileges("db1", VIRTUAL_VIEW, "v1");
    verifyRevokeOwnerPrivilegeStore("db1", VIRTUAL_VIEW, "v1", ROLE, "r1");
  }

  @Test
  public void testUnsupportedOwnerPrincipalType() {
    try {
      ownershipHandler.grantDatabaseOwnerPrivilege("d1", GROUP, "g1", WITH_GRANT);
      Assert.fail("GROUP should not be a valid owner type");
    } catch (Exception e) {
      assertEquals("Owner type not supported: GROUP", e.getMessage());
    }

    try {
      ownershipHandler.grantTableOwnerPrivilege("d1", MANAGED_TABLE, "t1", GROUP, "g1", WITH_GRANT);
      Assert.fail("GROUP should not be a valid owner type");
    } catch (Exception e) {
      assertEquals("Owner type not supported: GROUP", e.getMessage());
    }
  }

  private void stubListOwnersByAuthorizable(String dbName, String tableName,
    PrincipalType ownerType, String ownerName) throws Exception {

    TSentryAuthorizable authorizable = new TSentryAuthorizable();
    authorizable.setServer(AUTHZ_SERVER_NAME);
    authorizable.setDb(dbName);
    authorizable.setTable(tableName);

    Mockito.when(mockStore.listOwnersByAuthorizable(authorizable))
      .thenReturn(Collections.singletonList(new SentryOwnerInfo(SENTRY_PRINCIPAL_TYPE_MAP.get(ownerType), ownerName)));
  }


  private void verifyGrantOwnerPrivilegeStore(String dbName, String tableType, String tableName,
    PrincipalType ownerType, String ownerName, boolean grantOption) throws Exception {

    TSentryPrivilege privilege = createOwnerPrivilege(dbName, tableName, grantOption);

    Update update = null;
    if (tableName == null || !isVirtualView(tableType)) {
      update = createGrantOwnerUpdate(dbName, tableName,
        SENTRY_T_PRIVILEGE_PRINCIPAL_TYPE_MAP.get(ownerType), ownerName);
    }

    Mockito.verify(mockStore, Mockito.times(1))
      .alterSentryGrantOwnerPrivilege(ownerName, SENTRY_PRINCIPAL_TYPE_MAP.get(ownerType), privilege, update);

    Mockito.verifyNoMoreInteractions(mockStore);
    Mockito.reset(mockStore);
  }

  private void verifyRevokeOwnerPrivilegeStore(String dbName, String tableType, String tableName,
    PrincipalType ownerType, String ownerName) throws Exception {

    TSentryAuthorizable authorizable = new TSentryAuthorizable();
    authorizable.setServer(AUTHZ_SERVER_NAME);
    authorizable.setDb(dbName);
    authorizable.setTable(tableName);

    List<Update> update = null;
    if (tableName == null || !isVirtualView(tableType)) {
      update = Collections.singletonList(
        createRevokeOwnerUpdate(dbName, tableName,
          SENTRY_T_PRIVILEGE_PRINCIPAL_TYPE_MAP.get(ownerType), ownerName));

      Mockito.verify(mockStore, Mockito.times(1))
        .listOwnersByAuthorizable(authorizable);
    }

    Mockito.verify(mockStore, Mockito.times(1))
      .alterSentryRevokeOwnerPrivilege(authorizable, update);

    Mockito.verifyNoMoreInteractions(mockStore);
    Mockito.reset(mockStore);
  }

  private void verifyGrantOwnerPrivilegeAudit(String grantor, String dbName, String tableName,
    PrincipalType ownerType, String ownerName) {

    TSentryAuthorizable authorizable = new TSentryAuthorizable();
    authorizable.setServer(AUTHZ_SERVER_NAME);
    authorizable.setDb(dbName);
    authorizable.setTable(tableName);

    Mockito.verify(mockAudit, Mockito.times(1))
      .onGrantOwnerPrivilege(Status.OK(), grantor, SENTRY_T_PRINCIPAL_TYPE_MAP.get(ownerType), ownerName, authorizable);

    Mockito.verifyNoMoreInteractions(mockAudit);
    Mockito.reset(mockAudit);
  }

  private Update createGrantOwnerUpdate(String dbName, String tableName, TPrivilegePrincipalType ownerType, String ownerName) {
    String authzObj = dbName;
    if (tableName != null) {
      authzObj = authzObj + "." + tableName;
    }

    PermissionsUpdate update = new PermissionsUpdate();
    update.addPrivilegeUpdate(authzObj).putToAddPrivileges(
      new TPrivilegePrincipal(ownerType, ownerName), OWNER);
    return update;
  }

  private Update createRevokeOwnerUpdate(String dbName, String tableName, TPrivilegePrincipalType ownerType, String ownerName) {
    String authzObj = dbName;
    if (tableName != null) {
      authzObj = authzObj + "." + tableName;
    }

    PermissionsUpdate update = new PermissionsUpdate();
    update.addPrivilegeUpdate(authzObj).putToDelPrivileges(
      new TPrivilegePrincipal(ownerType, ownerName), OWNER);
    return update;
  }

  private TSentryPrivilege createOwnerPrivilege(String dbName, String tableName, boolean withGrant) {
    TSentryPrivilege ownerPrivilege = new TSentryPrivilege();
    ownerPrivilege.setAction(OWNER);

    if (tableName == null) {
      ownerPrivilege.setPrivilegeScope(PrivilegeScope.DATABASE.toString());
    } else {
      ownerPrivilege.setPrivilegeScope(PrivilegeScope.TABLE.toString());
    }

    ownerPrivilege.setServerName(AUTHZ_SERVER_NAME);
    ownerPrivilege.setDbName(dbName);
    ownerPrivilege.setTableName(tableName);

    // Default owner privilege is with no grant options
    ownerPrivilege.setGrantOption(
      (withGrant) ? TSentryGrantOption.TRUE : TSentryGrantOption.FALSE);

    return ownerPrivilege;
  }

  private boolean isVirtualView(String tableType) {
    return !Strings.isNullOrEmpty(tableType)
      && tableType.equalsIgnoreCase(TableType.VIRTUAL_VIEW.name());
  }
}

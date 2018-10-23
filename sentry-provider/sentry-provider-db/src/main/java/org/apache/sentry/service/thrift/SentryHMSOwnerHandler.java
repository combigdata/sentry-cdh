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

import static org.apache.sentry.core.model.db.AccessConstants.OWNER;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
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

/**
 * Grants and revokes OWNER privileges for HMS objects in the Sentry store. It also handles
 * the audit log messages for the owner grants.
 */
class SentryHMSOwnerHandler {
  private final SentryStoreInterface sentryStore;
  private final String AUTHZ_SERVER_NAME;
  private final SentryAuditLogger audit;
  private final String SENTRY_SERVICE_USER;

  private final static String NO_TABLE_TYPE = null;
  private final static String NO_TABLE = null;

  private static final Map<PrincipalType, TPrivilegePrincipalType> SENTRY_T_PRIVILEGE_PRINCIPAL_TYPE_MAP =
    ImmutableMap.of(
      PrincipalType.ROLE, TPrivilegePrincipalType.ROLE,
      PrincipalType.USER, TPrivilegePrincipalType.USER
    );

  private static final Map<PrincipalType, TSentryPrincipalType> SENTRY_T_PRINCIPAL_TYPE_MAP =
    ImmutableMap.of(
      PrincipalType.ROLE, TSentryPrincipalType.ROLE,
      PrincipalType.USER, TSentryPrincipalType.USER
    );

  private static final Map<PrincipalType, SentryPrincipalType> SENTRY_PRINCIPAL_TYPE_MAP =
      ImmutableMap.of(
      PrincipalType.ROLE, SentryPrincipalType.ROLE,
      PrincipalType.USER, SentryPrincipalType.USER
    );

  private static final Map<SentryPrincipalType, PrincipalType> PRINCIPAL_TYPE_MAP =
    ImmutableMap.of(
      SentryPrincipalType.ROLE, PrincipalType.ROLE,
      SentryPrincipalType.USER, PrincipalType.USER
    );

  /**
   * Constructs the handler with interfaces required to store and audit the owner privileges.
   *
   * <p/> All owner grants will have the authzServerName appended in the privilege.
   *
   * @param store The SentryStore interface where to store the owner privilege.
   * @param audit The Audit interface where to send the audit message.
   * @param authzServerName The server name linked to each of the owner privileges.
   * @param serviceUser The admin or service user who is granting these privileges (used only for
   * auditing messages).
   */
  SentryHMSOwnerHandler(SentryStoreInterface store, SentryAuditLogger audit, String authzServerName, String serviceUser) {
    this.sentryStore = store;
    this.AUTHZ_SERVER_NAME = authzServerName;
    this.audit = audit;
    this.SENTRY_SERVICE_USER = serviceUser;
  }

  /**
   * Grants ownership privileges on a database to an owner.
   *
   * @param dbName The database name to grant the owner privilege.
   * @param ownerType The principal type of the owner.
   * @param ownerName The name of the owner.
   * @param grantOption True if grant option must be granted; false otherwise.
   * @throws Exception If the owner privilege failed to be granted.
   */
  void grantDatabaseOwnerPrivilege(String dbName, PrincipalType ownerType, String ownerName, boolean grantOption)
    throws Exception {
    grantOwnerPrivilegeOnObject(dbName, NO_TABLE_TYPE, NO_TABLE, ownerType, ownerName, grantOption);
  }

  /**
   * Grants ownership privileges on a table to an owner.
   *
   * @param dbName The database name to grant the owner privilege.
   * @param tableType The table type to distinguish if a table is a VIRTUAL_VIEW or a simple table
   * @param tableName The table name to grant the owner privilege.
   * @param ownerType The principal type of the owner.
   * @param ownerName The name of the owner.
   * @param grantOption True if grant option must be granted; false otherwise.
   * @throws Exception If the owner privilege failed to be granted.
   */
  void grantTableOwnerPrivilege(String dbName, String tableType, String tableName, PrincipalType ownerType, String ownerName, boolean grantOption)
    throws Exception {
    grantOwnerPrivilegeOnObject(dbName, tableType, tableName, ownerType, ownerName, grantOption);
  }

  /**
   * Drops the owner privilege from a specific database.
   *
   * @param dbName The database name.
   * @throws Exception If the owner privilege cannot be dropped.
   */
  void dropDatabaseOwnerPrivileges(String dbName) throws Exception {
    dropOwnerPrivilegesFromObject(dbName, NO_TABLE_TYPE, NO_TABLE);
  }

  /**
   * Drops the owner privilege from a specific table.
   *
   * @param dbName The database name.
   * @param tableType The table type to distinguish if a table is a VIRTUAL_VIEW or a simple table
   * @param tableName The table name.
   * @throws Exception If the owner privilege cannot be dropped.
   */
  void dropTableOwnerPrivileges(String dbName, String tableType, String tableName) throws Exception {
    dropOwnerPrivilegesFromObject(dbName, tableType, tableName);
  }

  /**
   * Remove any OWNER privilege from the specified database (if tableName is null) or table.
   */
  private void dropOwnerPrivilegesFromObject(String dbName, String tableType, String tableName)
    throws Exception {

    String authzObj = toAuthzObjString(dbName, tableName);
    TSentryAuthorizable authorizable = toTSentryAuthorizable(dbName, tableName);

    List<Update> permissionUpdates = null;

    // Virtual views do not have physical locations
    if (tableName != NO_TABLE && !isVirtualView(tableType)) {
      permissionUpdates = new ArrayList<>();

      List<SentryOwnerInfo> ownerInfoList = sentryStore.listOwnersByAuthorizable(authorizable);
      for (SentryOwnerInfo ownerInfo : ownerInfoList) {
        permissionUpdates.add(createDeleteOwnerPermissionUpdate(authzObj,
          PRINCIPAL_TYPE_MAP.get(ownerInfo.getOwnerType()), ownerInfo.getOwnerName()));
      }
    }

    sentryStore.alterSentryRevokeOwnerPrivilege(authorizable, permissionUpdates);
  }

  /**
   * Grants ownership privileges on a database (if tableName is null) or table to an owner.
   */
  private void grantOwnerPrivilegeOnObject(String dbName, String tableType, String tableName,
    PrincipalType ownerType, String ownerName, boolean withGrant) throws Exception {

    if (!isPrincipalTypeSupported(ownerType)) {
      throw new Exception(String.format("Owner type not supported: %s", ownerType.toString()));
    }

    TSentryPrivilege ownerPrivilege = createOwnerPrivilege(dbName, tableName, withGrant);

    Update grantPermissionUpdate = null;

    // Create a permission update on HDFS for the owner privilege in the database
    // (Virtual views do not have physical locations)
    if (tableName != NO_TABLE && !isVirtualView(tableType)) {
      grantPermissionUpdate = createAddOwnerPermissionUpdate(toAuthzObjString(dbName, tableName),
        ownerType, ownerName);
    }

    // Store the owner privilege in the Sentry store
    sentryStore.alterSentryGrantOwnerPrivilege(ownerName, SENTRY_PRINCIPAL_TYPE_MAP.get(ownerType),
      ownerPrivilege, grantPermissionUpdate);

    // Send the new owner grant to the audit log file
    audit.onGrantOwnerPrivilege(Status.OK(), SENTRY_SERVICE_USER,
      SENTRY_T_PRINCIPAL_TYPE_MAP.get(ownerType), ownerName, toTSentryAuthorizable(dbName, tableName));
  }

  /**
   * Creates the TSentryPrivilege object for the OWNER privilege on the specified database (if
   * tableName is null) or table.
   */
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

  /**
   * Checks if the principal type is a supported principal. Only USER and ROLE are supported
   * principals for granting the OWNER privilege.
   */
  private boolean isPrincipalTypeSupported(PrincipalType principalType) {
    return (principalType == PrincipalType.ROLE || principalType == PrincipalType.USER);
  }

  /**
   * Creates the permission update for the OWNER privilege grant on the specified authorization object.
   */
  private Update createAddOwnerPermissionUpdate(String authzObj, PrincipalType ownerType, String ownerName) {
    PermissionsUpdate update = new PermissionsUpdate();
    update.addPrivilegeUpdate(authzObj).putToAddPrivileges(
      new TPrivilegePrincipal(SENTRY_T_PRIVILEGE_PRINCIPAL_TYPE_MAP.get(ownerType), ownerName), OWNER);
    return update;
  }

  /**
   * Creates the permission update for the OWNER privilege revoke on the specified authorization object.
   */
  private Update createDeleteOwnerPermissionUpdate(String authzObj, PrincipalType ownerType, String ownerName) {
    PermissionsUpdate update = new PermissionsUpdate();
    update.addPrivilegeUpdate(authzObj).putToDelPrivileges(
      new TPrivilegePrincipal(SENTRY_T_PRIVILEGE_PRINCIPAL_TYPE_MAP.get(ownerType), ownerName), OWNER);
    return update;
  }

  /**
   * Converts a database and/or table objects to the string form 'db.table'. If table is null, then
   * the string has only the database name in the form 'db' (no single quotes included).
   */
  private String toAuthzObjString(String dbName, String tableName) {
    if (tableName == null) {
      return dbName;
    }

    StringBuilder sb = new StringBuilder();
    sb.append(dbName).append(".").append(tableName);
    return sb.toString();
  }

  /**
   * Creates a TSentryAuthorizable object for the specified database and table names.
   */
  private TSentryAuthorizable toTSentryAuthorizable(String dbName, String tableName) {
    TSentryAuthorizable authorizable = new TSentryAuthorizable();
    authorizable.setServer(AUTHZ_SERVER_NAME);
    authorizable.setDb(dbName);
    authorizable.setTable(tableName);
    return authorizable;
  }

  private boolean isVirtualView(String tableType) {
    return !Strings.isNullOrEmpty(tableType)
      && tableType.equalsIgnoreCase(TableType.VIRTUAL_VIEW.name());
  }
}

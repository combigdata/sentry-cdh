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
package org.apache.sentry.provider.db.service.thrift;

import static org.apache.sentry.core.model.db.AccessConstants.ALL;
import static org.apache.sentry.core.model.db.AccessConstants.SELECT;

import com.codahale.metrics.Gauge;
import com.google.common.collect.Sets;
import java.util.Collections;
import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.SentryThriftAPIMismatchException;
import org.apache.sentry.core.common.utils.PolicyStoreConstants.PolicyStoreServerConfig;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.sentry.service.thrift.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestSentryPolicyStoreProcessor {

  private static final String SERVERNAME = "server1";
  private Configuration conf;
  private static final SentryStore sentryStore = Mockito.mock(SentryStore.class);
  private static final String ADMIN_GROUP = "admin_group";
  private static final String ADMIN_USER = "admin_user";

  @Before
  public void setup() {
    conf = new Configuration(true);

    Mockito.when(sentryStore.getRoleCountGauge()).thenReturn(new Gauge< Long >() {
      @Override
      public Long getValue() {
        return 0L;
      }
    });
    Mockito.when(sentryStore.getPrivilegeCountGauge()).thenReturn(new Gauge< Long >() {
      @Override
      public Long getValue() {
        return 0L;
      }
    });
    Mockito.when(sentryStore.getGroupCountGauge()).thenReturn(new Gauge< Long >() {
      @Override
      public Long getValue() {
        return 0L;
      }
    });
    Mockito.when(sentryStore.getHMSWaitersCountGauge()).thenReturn(new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return 0;
      }
    });
    Mockito.when(sentryStore.getLastNotificationIdGauge()).thenReturn(new Gauge< Long >() {
      @Override
      public Long getValue() {
        return 0L;
      }
    });    Mockito.when(sentryStore.getLastPathsSnapshotIdGauge()).thenReturn(new Gauge< Long >() {
      @Override
      public Long getValue() {
        return 0L;
      }
    });
    Mockito.when(sentryStore.getPermChangeIdGauge()).thenReturn(new Gauge< Long >() {
      @Override
      public Long getValue() {
        return 0L;
      }
    });
    Mockito.when(sentryStore.getPathChangeIdGauge()).thenReturn(new Gauge< Long >() {
      @Override
      public Long getValue() {
        return 0L;
      }
    });
  }

  @After
  public void reset () {
    Mockito.reset(sentryStore);
  }

  @Test(expected=SentryConfigurationException.class)
  public void testConfigNotNotificationHandler() throws Exception {
    conf.set(PolicyStoreServerConfig.NOTIFICATION_HANDLERS, Object.class.getName());
    SentryPolicyStoreProcessor.createHandlers(conf);
  }
  @Test(expected=SentryConfigurationException.class)
  public void testConfigCannotCreateNotificationHandler() throws Exception {
    conf.set(PolicyStoreServerConfig.NOTIFICATION_HANDLERS,
        ExceptionInConstructorNotificationHandler.class.getName());
    SentryPolicyStoreProcessor.createHandlers(conf);
  }
  @Test(expected=SentryConfigurationException.class)
  public void testConfigNotAClassNotificationHandler() throws Exception {
    conf.set(PolicyStoreServerConfig.NOTIFICATION_HANDLERS, "junk");
    SentryPolicyStoreProcessor.createHandlers(conf);
  }
  @Test
  public void testConfigMultipleNotificationHandlers() throws Exception {
    conf.set(PolicyStoreServerConfig.NOTIFICATION_HANDLERS,
        NoopNotificationHandler.class.getName() + "," +
            NoopNotificationHandler.class.getName() + " " +
            NoopNotificationHandler.class.getName());
    Assert.assertEquals(3, SentryPolicyStoreProcessor.createHandlers(conf).size());
  }
  public static class ExceptionInConstructorNotificationHandler extends NotificationHandler {
    public ExceptionInConstructorNotificationHandler(Configuration config) throws Exception {
      super(config);
      throw new Exception();
    }
  }
  public static class NoopNotificationHandler extends NotificationHandler {
    public NoopNotificationHandler(Configuration config) throws Exception {
      super(config);
    }
  }
  @Test(expected=SentryThriftAPIMismatchException.class)
  public void testSentryThriftAPIMismatch() throws Exception {
    SentryPolicyStoreProcessor.validateClientVersion(ServiceConstants.ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT -1);
  }
  @Test
  public void testSentryThriftAPIMatchVersion() throws Exception {
    SentryPolicyStoreProcessor.validateClientVersion(ServiceConstants.ThriftConstants.TSENTRY_SERVICE_VERSION_CURRENT);
  }

  @Test
  public void testGrantToRoleWithGrantCheck() throws Exception {
    final String DB = "db";
    final String ROLE = "role";
    final String USER = "user";

    MockGroupMappingService.addUserGroupMapping(ADMIN_USER, Sets.newHashSet(ADMIN_GROUP));

    Configuration conf = new Configuration();
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING, MockGroupMappingService.class.getName());
    conf.set(ServerConfig.ADMIN_GROUPS, ADMIN_GROUP);

    SentryPolicyStoreProcessorTestUtils test = new SentryPolicyStoreProcessorTestUtils(conf, sentryStore);

    final TSentryPrivilege ALL_ON_DB = test.newPrivilegeOnDatabase(ALL, SERVERNAME, DB);
    final TSentryPrivilege ALL_ON_DB_WGRANT = test.newPrivilegeOnDatabaseWithGrant(ALL, SERVERNAME, DB);
    final TSentryPrivilege SELECT_ON_DB = test.newPrivilegeOnDatabase(SELECT, SERVERNAME, DB);
    final TSentryPrivilege SELECT_ON_DB_WGRANT = test.newPrivilegeOnDatabaseWithGrant(SELECT, SERVERNAME, DB);

    // Admin user can grant privileges
    test.givenUser(ADMIN_USER).grantPrivilegeToRole(ALL_ON_DB, ROLE)
      .verify(Status.OK);
    test.givenUser(ADMIN_USER).grantPrivilegeToRole(ALL_ON_DB_WGRANT, ROLE)
      .verify(Status.OK);
    test.givenUser(ADMIN_USER).grantPrivilegeToRole(SELECT_ON_DB, ROLE)
      .verify(Status.OK);
    test.givenUser(ADMIN_USER).grantPrivilegeToRole(SELECT_ON_DB_WGRANT, ROLE)
      .verify(Status.OK);

    // User without grant option cannot grant privileges
    test.givenUser(USER).grantPrivilegeToRole(ALL_ON_DB, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.<TSentryPrivilege>emptySet())
      .verify(Status.ACCESS_DENIED);
    test.givenUser(USER).grantPrivilegeToRole(ALL_ON_DB_WGRANT, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.<TSentryPrivilege>emptySet())
      .verify(Status.ACCESS_DENIED);
    test.givenUser(USER).grantPrivilegeToRole(SELECT_ON_DB, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.<TSentryPrivilege>emptySet())
      .verify(Status.ACCESS_DENIED);
    test.givenUser(USER).grantPrivilegeToRole(SELECT_ON_DB_WGRANT, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.<TSentryPrivilege>emptySet())
      .verify(Status.ACCESS_DENIED);
    test.givenUser(USER).grantPrivilegeToRole(ALL_ON_DB, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(SELECT_ON_DB_WGRANT))
      .verify(Status.ACCESS_DENIED);

    // User with grant option can grant privileges
    test.givenUser(USER).grantPrivilegeToRole(ALL_ON_DB, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(ALL_ON_DB_WGRANT))
      .verify(Status.OK);
    test.givenUser(USER).grantPrivilegeToRole(SELECT_ON_DB, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(ALL_ON_DB_WGRANT))
      .verify(Status.OK);
    test.givenUser(USER).grantPrivilegeToRole(SELECT_ON_DB, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(SELECT_ON_DB_WGRANT))
      .verify(Status.OK);

    // User with grant option can grant privileges with grant option
    test.givenUser(USER).grantPrivilegeToRole(ALL_ON_DB_WGRANT, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(ALL_ON_DB_WGRANT))
      .verify(Status.OK);
    test.givenUser(USER).grantPrivilegeToRole(SELECT_ON_DB_WGRANT, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(ALL_ON_DB_WGRANT))
      .verify(Status.OK);
    test.givenUser(USER).grantPrivilegeToRole(SELECT_ON_DB_WGRANT, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(SELECT_ON_DB_WGRANT))
      .verify(Status.OK);
  }

  @Test
  public void testRevokeFromRoleWithGrantCheck() throws Exception {
    final String DB = "db";
    final String ROLE = "role";
    final String USER = "user";

    MockGroupMappingService.addUserGroupMapping(ADMIN_USER, Sets.newHashSet(ADMIN_GROUP));

    Configuration conf = new Configuration();
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING, MockGroupMappingService.class.getName());
    conf.set(ServerConfig.ADMIN_GROUPS, ADMIN_GROUP);

    SentryPolicyStoreProcessorTestUtils test = new SentryPolicyStoreProcessorTestUtils(conf, sentryStore);

    final TSentryPrivilege ALL_ON_DB = test.newPrivilegeOnDatabase(ALL, SERVERNAME, DB);
    final TSentryPrivilege ALL_ON_DB_WGRANT = test.newPrivilegeOnDatabaseWithGrant(ALL, SERVERNAME, DB);
    final TSentryPrivilege SELECT_ON_DB = test.newPrivilegeOnDatabase(SELECT, SERVERNAME, DB);
    final TSentryPrivilege SELECT_ON_DB_WGRANT = test.newPrivilegeOnDatabaseWithGrant(SELECT, SERVERNAME, DB);

    // Admin user can revoke privileges
    test.givenUser(ADMIN_USER).revokePrivilegeFromRole(ALL_ON_DB, ROLE)
      .verify(Status.OK);
    test.givenUser(ADMIN_USER).revokePrivilegeFromRole(ALL_ON_DB_WGRANT, ROLE)
      .verify(Status.OK);
    test.givenUser(ADMIN_USER).revokePrivilegeFromRole(SELECT_ON_DB, ROLE)
      .verify(Status.OK);
    test.givenUser(ADMIN_USER).revokePrivilegeFromRole(SELECT_ON_DB_WGRANT, ROLE)
      .verify(Status.OK);

    // User without grant option cannot revoke privileges
    test.givenUser(USER).revokePrivilegeFromRole(ALL_ON_DB, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.<TSentryPrivilege>emptySet())
      .verify(Status.ACCESS_DENIED);
    test.givenUser(USER).revokePrivilegeFromRole(ALL_ON_DB_WGRANT, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.<TSentryPrivilege>emptySet())
      .verify(Status.ACCESS_DENIED);
    test.givenUser(USER).revokePrivilegeFromRole(SELECT_ON_DB, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.<TSentryPrivilege>emptySet())
      .verify(Status.ACCESS_DENIED);
    test.givenUser(USER).revokePrivilegeFromRole(SELECT_ON_DB_WGRANT, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.<TSentryPrivilege>emptySet())
      .verify(Status.ACCESS_DENIED);
    test.givenUser(USER).revokePrivilegeFromRole(ALL_ON_DB, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(SELECT_ON_DB_WGRANT))
      .verify(Status.ACCESS_DENIED);

    // User with grant option can revoke privileges
    test.givenUser(USER).revokePrivilegeFromRole(ALL_ON_DB, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(ALL_ON_DB_WGRANT))
      .verify(Status.OK);
    test.givenUser(USER).revokePrivilegeFromRole(SELECT_ON_DB, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(ALL_ON_DB_WGRANT))
      .verify(Status.OK);
    test.givenUser(USER).revokePrivilegeFromRole(SELECT_ON_DB, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(SELECT_ON_DB_WGRANT))
      .verify(Status.OK);

    // User with grant option can revoke privileges with grant option
    test.givenUser(USER).revokePrivilegeFromRole(ALL_ON_DB_WGRANT, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(ALL_ON_DB_WGRANT))
      .verify(Status.OK);
    test.givenUser(USER).revokePrivilegeFromRole(SELECT_ON_DB_WGRANT, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(ALL_ON_DB_WGRANT))
      .verify(Status.OK);
    test.givenUser(USER).revokePrivilegeFromRole(SELECT_ON_DB_WGRANT, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(SELECT_ON_DB_WGRANT))
      .verify(Status.OK);
  }

  @Test
  public void testGrantCheckWithColumn() throws Exception {
    final String DB = "db";
    final String TABLE = "table";
    final String COLUMN = "column";
    final String ROLE = "role";
    final String USER = "user";

    MockGroupMappingService.addUserGroupMapping(ADMIN_USER, Sets.newHashSet(ADMIN_GROUP));

    Configuration conf = new Configuration();
    conf.set(ServerConfig.SENTRY_STORE_GROUP_MAPPING, MockGroupMappingService.class.getName());
    conf.set(ServerConfig.ADMIN_GROUPS, ADMIN_GROUP);

    SentryPolicyStoreProcessorTestUtils test = new SentryPolicyStoreProcessorTestUtils(conf, sentryStore);

    final TSentryPrivilege SELECT_ON_TABLE = test.newPrivilegeOnTable(SELECT, SERVERNAME, DB, TABLE);
    final TSentryPrivilege SELECT_ON_TABLE_WGRANT = test.newPrivilegeOnTableWithGrant(SELECT, SERVERNAME, DB, TABLE);
    final TSentryPrivilege SELECT_ON_COLUMN = test.newPrivilegeOnColumn(SELECT, SERVERNAME, DB, TABLE, COLUMN);

    // Admin user can revoke privileges
    test.givenUser(ADMIN_USER).grantPrivilegeToRole(SELECT_ON_TABLE_WGRANT, ROLE)
      .verify(Status.OK);
    test.givenUser(ADMIN_USER).grantPrivilegeToRole(SELECT_ON_COLUMN, ROLE)
      .verify(Status.OK);

    // User with grant option on table can grant select on a column
    test.givenUser(USER).grantPrivilegeToRole(SELECT_ON_COLUMN, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(SELECT_ON_TABLE_WGRANT))
      .verify(Status.OK);

    // User without grant option on table cannot grant select on a column
    test.givenUser(USER).grantPrivilegeToRole(SELECT_ON_COLUMN, ROLE)
      .whenRequestStorePrivilegesReturn(Collections.singleton(SELECT_ON_TABLE))
      .verify(Status.ACCESS_DENIED);
  }
}

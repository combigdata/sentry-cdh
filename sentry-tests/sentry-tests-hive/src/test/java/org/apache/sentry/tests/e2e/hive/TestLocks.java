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

package org.apache.sentry.tests.e2e.hive;

import java.sql.Connection;
import java.sql.Statement;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager;
import org.apache.sentry.provider.file.PolicyFile;
import org.junit.Before;
import org.junit.Test;

public class TestLocks extends AbstractTestWithStaticConfiguration {

  private PolicyFile policyFile;

  @Before
  public void setup() throws Exception {
    policyFile = PolicyFile.setAdminOnServer1(ADMINGROUP);
  }

  @Test
  public void testLockTable() throws Exception {

    policyFile
        .addRolesToGroup(USERGROUP1, "select_tab1")
        .addPermissionsToRole("select_tab1",
            "server=server1->db=DB_1->table=TAB_1->action=select")
        .setUserGroupMapping(StaticUserGroup.getStaticMapping());
    writePolicyFile(policyFile);

    // setup db objects needed by the test
    Connection connection = context.createConnection(ADMIN1);
    Statement statement = context.createStatement(connection);

    statement.execute("DROP DATABASE IF EXISTS DB_1 CASCADE");
    statement.execute("CREATE DATABASE " + DB1);
    statement.execute("USE " + DB1);
    statement.execute("CREATE TABLE TAB_1(B INT, A STRING) "
        + " row format delimited fields terminated by '|'  stored as textfile");
    statement.close();
    connection.close();

    connection = context.createConnection(USER1_1);
    statement = context.createStatement(connection);
    statement.execute("SET "
        + HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname + "=true");
    statement.execute("SET " + HiveConf.ConfVars.HIVE_LOCK_MANAGER.varname
        + "=" + EmbeddedLockManager.class.getName());
    statement.execute("LOCK TABLE " + DB1 + ".TAB_1 EXCLUSIVE");
    // Workaround for Hive's lock handling.
    // verify that unlock failure is not due to authorization
    context.assertAuthzExecHookException(statement, "UNLOCK TABLE " + DB1 + ".TAB_1");

    statement.execute("USE " + DB1);
    statement.execute("LOCK TABLE TAB_1 EXCLUSIVE");
    context.assertAuthzExecHookException(statement, "UNLOCK TABLE TAB_1");

    statement.close();
    connection.close();

    // test cleanup
    connection = context.createConnection(ADMIN1);
    statement = context.createStatement(connection);
    statement.execute("DROP DATABASE DB_1 CASCADE");
    statement.close();
    connection.close();
  }

}

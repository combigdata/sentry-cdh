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

package org.apache.sentry.provider.db.generic.tools;

import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClientFactory;
import org.apache.sentry.provider.db.generic.tools.command.*;
import org.apache.sentry.provider.db.tools.SentryShellCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sentry.provider.common.AuthorizationComponent.HBASEINDEXER;
import static org.apache.sentry.service.thrift.ServiceConstants.ClientConfig.SERVICE_NAME;

/**
 * SentryShellIndexer is an admin tool, and responsible for the management of repository.
 * The following commands are supported:
 * create role, drop role, add group to role, grant privilege to role,
 * revoke privilege from role, list roles, list privilege for role.
 */
public class SentryShellIndexer extends SentryShellCommon {

  protected boolean isMigration = false;

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryShellIndexer.class);

  private final SentryConfigToolIndexer configTool = new SentryConfigToolIndexer();

  @Override
  protected void setupOptions(Options simpleShellOptions) {
    super.setupOptions(simpleShellOptions);
    configTool.setupOptions(simpleShellOptions);
  }

  @Override
  protected void parseOptions(CommandLine cmd) throws ParseException {
    super.parseOptions(cmd);
    configTool.parseOptions(cmd);
    for (Option opt : cmd.getOptions()) {
      if (opt.getOpt().equals("mgr")) {
        isMigration = true;
      }
    }
  }

  @Override
  protected OptionGroup getMainOptions() {
    OptionGroup mainOptions = super.getMainOptions();
    Option mgrOpt = new Option("mgr", "migrate", false, "Migrate ini file to Sentry service");
    mgrOpt.setRequired(false);
    mainOptions.addOption(mgrOpt);
    return mainOptions;
  }

  @Override
  public void run() throws Exception {

    if (isMigration) {
      configTool.run();
      return;
    }

    Command command = null;
    String component = HBASEINDEXER;
    Configuration conf = getSentryConf();

    String service = conf.get(SERVICE_NAME, serviceName);

    if (service == null) {
      throw new IllegalArgumentException("Service was not defined. Please, use -s command option, or sentry.provider.backend.generic.service-name configuration entry.");
    }

    LOGGER.info(String.format("Context: component=%s, service=%s", component, service));
    try (SentryGenericServiceClient client =
                 SentryGenericServiceClientFactory.create(conf)) {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      String requestorName = ugi.getShortUserName();

      if (isCreateRole) {
        command = new CreateRoleCmd(roleName, component);
      } else if (isDropRole) {
        command = new DropRoleCmd(roleName, component);
      } else if (isAddRoleGroup) {
        command = new AddRoleToGroupCmd(roleName, groupName, component);
      } else if (isDeleteRoleGroup) {
        command = new DeleteRoleFromGroupCmd(roleName, groupName, component);
      } else if (isGrantPrivilegeRole) {
        command = new GrantPrivilegeToRoleCmd(roleName, component,
                privilegeStr, new IndexerTSentryPrivilegeConverter(component, service));
      } else if (isRevokePrivilegeRole) {
        command = new RevokePrivilegeFromRoleCmd(roleName, component,
                privilegeStr, new IndexerTSentryPrivilegeConverter(component, service));
      } else if (isListRole) {
        command = new ListRolesCmd(groupName, component);
      } else if (isListPrivilege) {
        command = new ListPrivilegesByRoleCmd(roleName, component,
                service, new SolrTSentryPrivilegeConverter(component, service));
      }

      // check the requestor name
      if (StringUtils.isEmpty(requestorName)) {
        // The exception message will be recorded in log file.
        throw new Exception("The requestor name is empty.");
      }

      if (command != null) {
        command.execute(client, requestorName);
      }
    }
  }

  private Configuration getSentryConf() {
    Configuration conf = new Configuration();
    conf.addResource(new Path(confPath));
    return conf;
  }

  public static void main(String[] args) throws Exception {
    SentryShellIndexer sentryShell = new SentryShellIndexer();
    try {
      sentryShell.executeShell(args);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      Throwable current = e;
      // find the first printable message;
      while (current != null && current.getMessage() == null) {
        current = current.getCause();
      }
      System.out.println("The operation failed." +
              (current.getMessage() == null ? "" : "  Message: " + current.getMessage()));
      System.exit(1);
    }
  }

}

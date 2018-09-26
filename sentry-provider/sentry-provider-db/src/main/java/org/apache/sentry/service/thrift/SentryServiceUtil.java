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

package org.apache.sentry.service.thrift;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SYNC_ALTER_WITH_POLICY_STORE;
import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SYNC_CREATE_WITH_POLICY_STORE;
import static org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars.AUTHZ_SYNC_DROP_WITH_POLICY_STORE;
import static org.apache.sentry.service.thrift.ServiceConstants.ServerConfig.SENTRY_DB_EXPLICIT_GRANTS_PERMITTED;
import static org.apache.sentry.service.thrift.ServiceConstants.ServerConfig.SENTRY_DB_EXPLICIT_GRANTS_PERMITTED_DEFAULT;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.sentry.provider.db.SentryGrantDeniedException;
import org.apache.sentry.provider.db.SentryInvalidInputException;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.provider.db.service.persistent.SentryStoreInterface;
import org.apache.sentry.provider.db.service.thrift.TSentryAuthorizable;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.slf4j.Logger;

public final class SentryServiceUtil {
  private static boolean firstCallHDFSSyncEnabled = true;
  private static boolean hdfsSyncEnabled = false;

  /**
   * Gracefully shut down an Executor service.
   * <p>
   * This code is based on the Javadoc example for the Executor service.
   * <p>
   * First call shutdown to reject incoming tasks, and then call
   * shutdownNow, if necessary, to cancel any lingering tasks.
   *
   * @param pool the executor service to shut down
   * @param poolName the name of the executor service to shut down to make it easy for debugging
   * @param timeout the timeout interval to wait for its termination
   * @param unit the unit of the timeout
   * @param logger the logger to log the error message if it cannot terminate. It could be null
   */
  public static void shutdownAndAwaitTermination(ExecutorService pool, String poolName,
                       long timeout, TimeUnit unit, Logger logger) {
    Preconditions.checkNotNull(pool);

    pool.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(timeout, unit)) {
        pool.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if ((!pool.awaitTermination(timeout, unit)) && (logger != null)) {
          logger.error("Executor service {} did not terminate",
              StringUtils.defaultIfBlank(poolName, "null"));
        }
      }
    } catch (InterruptedException ignored) {
      // (Re-)Cancel if current thread also interrupted
      pool.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Check if Sentry is configured with HDFS sync enabled. Cache the result
   *
   * @param conf The Configuration object where HDFS sync configurations are set.
   * @return True if enabled; False otherwise.
   */
  public static boolean isHDFSSyncEnabled(Configuration conf) {
    if (firstCallHDFSSyncEnabled) {
      List<String> processorFactories =
          Arrays.asList(conf.get(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "").split(","));

      List<String> policyStorePlugins =
          Arrays.asList(
              conf.get(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "").split(","));

      hdfsSyncEnabled =
          processorFactories.contains("org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory")
              && policyStorePlugins.contains("org.apache.sentry.hdfs.SentryPlugin");
      firstCallHDFSSyncEnabled = false;
    }

    return hdfsSyncEnabled;
  }

    /**
     * Check if Sentry is configured with HDFS sync enabled without caching the result
     *
     * @param conf The Configuration object where HDFS sync configurations are set.
     * @return True if enabled; False otherwise.
     */
  public static boolean isHDFSSyncEnabledNoCache(Configuration conf) {

    List<String> processorFactories =
        Arrays.asList(conf.get(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "").split(","));

    List<String> policyStorePlugins =
        Arrays.asList(
            conf.get(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "").split(","));

    hdfsSyncEnabled =
        processorFactories.contains("org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory")
            && policyStorePlugins.contains("org.apache.sentry.hdfs.SentryPlugin");


    return hdfsSyncEnabled;
  }

  /**
   * Check if Sentry is configured with policy store sync enabled
   * @param conf
   * @return True if enabled; False otherwise
   */
  public static boolean isSyncPolicyStoreEnabled(Configuration conf) {
    boolean syncStoreOnCreate;
    boolean syncStoreOnDrop;
    boolean syncStoreOnAlter;

    syncStoreOnCreate  = Boolean
        .parseBoolean(conf.get(AUTHZ_SYNC_CREATE_WITH_POLICY_STORE.getVar(),
            AUTHZ_SYNC_CREATE_WITH_POLICY_STORE.getDefault()));
    syncStoreOnDrop = Boolean.parseBoolean(conf.get(AUTHZ_SYNC_DROP_WITH_POLICY_STORE.getVar(),
        AUTHZ_SYNC_DROP_WITH_POLICY_STORE.getDefault()));
    syncStoreOnAlter = Boolean.parseBoolean(conf.get(AUTHZ_SYNC_ALTER_WITH_POLICY_STORE.getVar(),
        AUTHZ_SYNC_ALTER_WITH_POLICY_STORE.getDefault()));

    return syncStoreOnCreate || syncStoreOnDrop || syncStoreOnAlter;
  }

  static String getHiveMetastoreURI() {
    HiveConf hiveConf = new HiveConf();
    return hiveConf.get(METASTOREURIS.varname);
  }

  /**
   * Derives object name from database and table names by concatenating them
   *
   * @param authorizable for which is name is to be derived
   * @return authorizable name
   * @throws SentryInvalidInputException if argument provided does not have all the
   *                                     required fields set.
   */
  public static String getAuthzObj(TSentryAuthorizable authorizable)
    throws SentryInvalidInputException {
    return getAuthzObj(authorizable.getDb(), authorizable.getTable());
  }

  /**
   * Derives object name from database and table names by concatenating them
   *
   * @param dbName
   * @param tblName
   * @return authorizable name
   * @throws SentryInvalidInputException if argument provided does not have all the
   *                                     required fields set.
   */
  public static String getAuthzObj(String dbName, String tblName)
    throws SentryInvalidInputException {
    if (SentryStore.isNULL(dbName)) {
      throw new SentryInvalidInputException("Invalif input, DB name is missing");
    }
    return SentryStore.isNULL(tblName) ? dbName.toLowerCase() :
      (dbName + "." + tblName).toLowerCase();
  }

  private SentryServiceUtil() {
    // Make constructor private to avoid instantiation
  }

  /**
   * Checks if a list of privileges are permitted to be explicitly granted by any of the Sentry DB
   * clients.
   * <p/>
   * The list of privileges are checked against the configuration 'sentry.db.explicit.grants.permitted'
   * that should exist in the Configuration object. This configuration has a list of comma-separated
   * privileges. If an empty value is set, then it allows any privilege to be granted.
   *
   * @param conf The Configuration object that has the key=value of the privileges that are permitted.
   * @param privileges A set of privileges that need to be verified if are permitted.
   * @throws SentryGrantDeniedException If at least one of the privileges in the set is not permitted.
   */
  public static void checkDbExplicitGrantsPermitted(Configuration conf, Set<TSentryPrivilege> privileges)
    throws SentryGrantDeniedException {
    Set<String> permittedGrants = getDbGrantsPermittedFromConf(conf);
    if (permittedGrants.isEmpty()) {
      return;
    }

    Set<String> deniedGrants = new HashSet<>();
    for (TSentryPrivilege privilege : privileges) {
      String action = privilege.getAction();
      if (action != null) {
        action = action.trim().toUpperCase();

        // Will collect all grants not permitted so the exception thrown displays which privileges
        // cannot be granted
        if (!permittedGrants.contains(action)) {
          deniedGrants.add(action);
        }
      }
    }

    if (!deniedGrants.isEmpty()) {
      throw new SentryGrantDeniedException(
        String.format("GRANT privilege for %s not permitted.", deniedGrants));
    }
  }

  // Returns the list of privileges found on the Configuration object that are permitted to be
  // granted.
  // The returned Set has all privileges in upper case and spaces trimmed to avoid mistakes
  // during comparison.
  private static Set<String> getDbGrantsPermittedFromConf(Configuration conf) {
    String grantsConfig = conf.get(SENTRY_DB_EXPLICIT_GRANTS_PERMITTED,
      SENTRY_DB_EXPLICIT_GRANTS_PERMITTED_DEFAULT).trim();

    if (grantsConfig.isEmpty()) {
      return Collections.emptySet();
    }

    Set<String> permittedGrants = new HashSet<>();
    for (String grant : grantsConfig.split(",")) {
      permittedGrants.add(grant.trim().toUpperCase());
    }

    return permittedGrants;
  }

  /**
   * Returns a new SentryStoreInterface instance based on the sentry.service.sentrystore property
   * configuration.
   *
   * @param conf The Configuration object where the sentry.service.sentrystore property is set.
   * @param logger The Log4j logger object used to log info/warn/error messages.
   * @return A new SentryStoreInterface implementation.
   */
  public static SentryStoreInterface getSentryStore(Configuration conf, Logger logger) {
    String sentryStoreClass = conf.get(ServerConfig.SENTRY_STORE,
      ServerConfig.SENTRY_STORE_DEFAULT);
    try {
      Class<?> sentryClazz = conf.getClassByName(sentryStoreClass);
      Constructor<?> sentryConstructor = sentryClazz
        .getConstructor(Configuration.class);
      Object sentryObj = sentryConstructor.newInstance(conf);
      if (sentryObj instanceof SentryStoreInterface) {
        logger.info("Instantiating sentry store class: " + sentryStoreClass);
        return (SentryStoreInterface) sentryConstructor.newInstance(conf);
      }
      // The supplied class doesn't implement SentryStoreIface. Let's try to use a proxy
      // instance.
      // In practice, the following should only be used in development phase, as there are
      // cases where using a proxy can fail, and result in runtime errors.
      logger.warn(
        String.format("Trying to use a proxy instance (duck-typing) for the " +
          "supplied SentryStore, since the specified class %s does not implement " +
          "SentryStoreIface.", sentryStoreClass));
      return
        new DynamicProxy<>(sentryObj, SentryStoreInterface.class, sentryStoreClass).createProxy();
    } catch (Exception e) {
      throw new IllegalStateException("Could not create "
        + sentryStoreClass, e);
    }
  }
}

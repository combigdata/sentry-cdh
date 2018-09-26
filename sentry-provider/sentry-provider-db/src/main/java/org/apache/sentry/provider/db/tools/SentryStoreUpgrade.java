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
package org.apache.sentry.provider.db.tools;

import org.apache.sentry.provider.db.service.persistent.SentryStoreInterface;

/**
 * Interface used by the SentrySchemaTool class to execute any upgrade steps after the
 * Sentry schema upgrade is completed.
 */
public interface SentryStoreUpgrade {

  /**
   * Checks if a version upgrade from requires the execution of this upgrade step.
   *
   * @param fromVersion The schema version which Sentry has upgraded from.
   * @return True if it requires an upgrade; False otherwise.
   */
  boolean needsUpgrade(String fromVersion);

  /**
   * Executes a post-schema upgrade step on the Sentry Store DB.
   *
   * @param store The Sentry store where to execute the DB operations.
   * @param userName The user who is executing this upgrade (usually the admin user).
   * @throws Exception If an error has occurred during the upgrade.
   */
  void upgrade(String userName, SentryStoreInterface store) throws Exception;
}

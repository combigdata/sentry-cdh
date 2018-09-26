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
package org.apache.sentry.provider.db.service.persistent;

import java.lang.reflect.Constructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.apache.sentry.service.thrift.DynamicProxy;
import org.slf4j.Logger;

/**
 * Factory class used to create an instance of the SentryStoreInterface.
 */
public class SentryStoreFactory {
  /**
   * Returns a SentryStoreInterface instance based on the sentry.service.sentrystore property
   * configuration.
   *
   * @param conf The Configuration object where the sentry.service.sentrystore property is set.
   * @param logger The Log4j logger object used to log info/warn/error messages.
   * @return A new SentryStoreInterface implementation.
   */
  public static SentryStoreInterface create(Configuration conf, Logger logger) {
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

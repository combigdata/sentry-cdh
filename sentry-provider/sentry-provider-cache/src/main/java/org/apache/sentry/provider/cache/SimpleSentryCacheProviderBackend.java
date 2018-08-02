/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.cache;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.provider.common.ProviderBackendContext;

import java.util.Set;

public class SimpleSentryCacheProviderBackend extends SimpleCacheProviderBackend {

  public SimpleSentryCacheProviderBackend(Configuration conf, String resourcePath) {
    super(conf, resourcePath);
  }

 /*
  This is temporary change as Impala does not have implementation for user privileges.
  Once Impala has support for user privileges this API should be moved to SimpleCacheProviderBackend.
  TODO Move this API back to SimpleCacheProviderBackend once IMPALA-7343 is resolved.
 */
  @Override
  public ImmutableSet<String> getPrivileges(Set<String> groups, Set<String> users,
                                            ActiveRoleSet roleSet, Authorizable... authorizableHierarchy) {
    if (!initialized()) {
      throw new IllegalStateException(
              "Backend has not been properly initialized");
    }
    return ImmutableSet.copyOf(((SentryPrivilegeCache) cacheHandle).listPrivileges(groups, users,
            roleSet));
  }
}

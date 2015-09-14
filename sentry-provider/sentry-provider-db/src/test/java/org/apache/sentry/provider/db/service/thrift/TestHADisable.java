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

import junit.framework.Assert;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Sentry HA is disabled
 */
public class TestHADisable extends SentryServiceIntegrationBase {

  @BeforeClass
  public static void setup() throws Exception {

  }

  @Override
  @Before
  public void before() throws Exception {
  }

  @Override
  @After
  public void after() throws SentryUserException {
  }

  @Test
  public void testSentryFailsToStartWithHa() throws Exception {
    haEnabled = true;
    try {
      setupConf();
      Assert.fail("Expected Sentry startup to fail when HA is enabled");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof UnsupportedOperationException);
    }
  }

}

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
package org.apache.sentry.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.hdfs.service.thrift.TAuthzUpdateRequest;
import org.apache.sentry.hdfs.service.thrift.TAuthzUpdateResponse;
import org.apache.sentry.provider.db.SentryPolicyStorePlugin;
import org.apache.sentry.provider.db.service.model.MSentryPathChange;
import org.apache.sentry.provider.db.service.model.MSentryPermChange;
import org.apache.sentry.provider.db.service.persistent.PathsImage;
import org.apache.sentry.provider.db.service.persistent.PermissionsImage;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestSentryHDFSServiceProcessor {
  private static SentryHDFSServiceProcessor serviceProcessor;
  private static SentryStore sentryStoreMock;

  @BeforeClass
  public static void setUp() throws SentryPolicyStorePlugin.SentryPluginException {
    serviceProcessor = new SentryHDFSServiceProcessor();
    sentryStoreMock = Mockito.mock(SentryStore.class);
    new SentryPlugin().initialize(new Configuration(), sentryStoreMock);
  }

  @Test
  @Ignore
  public void testInitialHDFSSyncReturnsAFullImage() throws Exception {
    Mockito.when(sentryStoreMock.getLastProcessedImageID())
        .thenReturn(1L);
    Mockito.when(sentryStoreMock.retrieveFullPathsImage())
        .thenReturn(new PathsImage(new HashMap<String, Collection<String>>(), 1, 1));

    Mockito.when(sentryStoreMock.getLastProcessedPermChangeID())
        .thenReturn(1L);
    Mockito.when(sentryStoreMock.retrieveFullPermssionsImage())
        .thenReturn(new PermissionsImage(new HashMap<String, List<String>>(), new HashMap<String, Map<String, String>>(), 1));

    TAuthzUpdateRequest updateRequest = new TAuthzUpdateRequest(1, 1, 0);
    TAuthzUpdateResponse sentryUpdates= serviceProcessor.get_authz_updates(updateRequest);

    assertEquals(1, sentryUpdates.getAuthzPathUpdateSize());
    assertEquals(1, sentryUpdates.getAuthzPathUpdate().get(0).getImgNum());
    assertEquals(1, sentryUpdates.getAuthzPathUpdate().get(0).getSeqNum());
    assertTrue(sentryUpdates.getAuthzPathUpdate().get(0).isHasFullImage());

    assertEquals(1, sentryUpdates.getAuthzPermUpdateSize());
    assertEquals(1, sentryUpdates.getAuthzPermUpdate().get(0).getSeqNum());
    assertTrue(sentryUpdates.getAuthzPermUpdate().get(0).isHasfullImage());
  }

  @Test
  @Ignore
  public void testRequestSyncUpdatesWhenNewImagesArePersistedReturnsANewFullImage() throws Exception {
    Mockito.when(sentryStoreMock.getLastProcessedImageID())
        .thenReturn(2L);
    Mockito.when(sentryStoreMock.retrieveFullPathsImage())
        .thenReturn(new PathsImage(new HashMap<String, Collection<String>>(), 3, 2));

    Mockito.when(sentryStoreMock.getLastProcessedPermChangeID())
        .thenReturn(3L);
    Mockito.when(sentryStoreMock.retrieveFullPermssionsImage())
        .thenReturn(new PermissionsImage(new HashMap<String, List<String>>(), new HashMap<String, Map<String, String>>(), 3));

    TAuthzUpdateRequest updateRequest = new TAuthzUpdateRequest(2, 2, 1);
    TAuthzUpdateResponse sentryUpdates= serviceProcessor.get_authz_updates(updateRequest);

    assertEquals(1, sentryUpdates.getAuthzPathUpdateSize());
    assertEquals(2, sentryUpdates.getAuthzPathUpdate().get(0).getImgNum());
    assertEquals(3, sentryUpdates.getAuthzPathUpdate().get(0).getSeqNum());
    assertTrue(sentryUpdates.getAuthzPathUpdate().get(0).isHasFullImage());

    assertEquals(1, sentryUpdates.getAuthzPermUpdateSize());
    assertEquals(3, sentryUpdates.getAuthzPermUpdate().get(0).getSeqNum());
    assertTrue(sentryUpdates.getAuthzPermUpdate().get(0).isHasfullImage());
  }

  @Test
  public void testRequestSyncUpdatesWhenNewDeltasArePersistedReturnsDeltaChanges() throws Exception {
    Mockito.when(sentryStoreMock.getLastProcessedImageID())
        .thenReturn(1L);
    Mockito.when(sentryStoreMock.getLastProcessedPathChangeID())
        .thenReturn(3L);
    Mockito.when(sentryStoreMock.pathChangeExists(2))
        .thenReturn(true);
    Mockito.when(sentryStoreMock.getMSentryPathChanges(2))
        .thenReturn(Arrays.asList(new MSentryPathChange(3, "u3", new PathsUpdate(3, 1, false))));

    Mockito.when(sentryStoreMock.getLastProcessedPermChangeID())
        .thenReturn(3L);
    Mockito.when(sentryStoreMock.permChangeExists(2))
        .thenReturn(true);
    Mockito.when(sentryStoreMock.getMSentryPermChanges(2))
        .thenReturn(Arrays.asList(new MSentryPermChange(3, new PermissionsUpdate(3, false))));

    TAuthzUpdateRequest updateRequest = new TAuthzUpdateRequest(2, 2, 1);
    TAuthzUpdateResponse sentryUpdates= serviceProcessor.get_authz_updates(updateRequest);

    assertEquals(1, sentryUpdates.getAuthzPathUpdateSize());
    assertEquals(1, sentryUpdates.getAuthzPathUpdate().get(0).getImgNum());
    assertEquals(3, sentryUpdates.getAuthzPathUpdate().get(0).getSeqNum());
    assertFalse(sentryUpdates.getAuthzPathUpdate().get(0).isHasFullImage());

    assertEquals(1, sentryUpdates.getAuthzPermUpdateSize());
    assertEquals(3, sentryUpdates.getAuthzPermUpdate().get(0).getSeqNum());
    assertFalse(sentryUpdates.getAuthzPermUpdate().get(0).isHasfullImage());
  }

  @Test
  public void testRequestSyncUpdatesWhenNoUpdatesExistReturnsEmptyResults() throws Exception {
    Mockito.when(sentryStoreMock.getLastProcessedImageID())
        .thenReturn(1L);
    Mockito.when(sentryStoreMock.getLastProcessedPathChangeID())
        .thenReturn(2L);
    Mockito.when(sentryStoreMock.getLastProcessedPermChangeID())
        .thenReturn(2L);

    TAuthzUpdateRequest updateRequest = new TAuthzUpdateRequest(3, 3, 1);
    TAuthzUpdateResponse sentryUpdates= serviceProcessor.get_authz_updates(updateRequest);

    assertEquals(0, sentryUpdates.getAuthzPathUpdateSize());
    assertEquals(0, sentryUpdates.getAuthzPermUpdateSize());
  }
}

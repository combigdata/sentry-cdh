/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  <p>
  http://www.apache.org/licenses/LICENSE-2.0
  <p>
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

package org.apache.sentry.service.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.junit.Test;
import org.mockito.Mockito;

public class TestHiveNotificationFetcher {
  @Test
  public void testGetEmptyNotificationsWhenHmsReturnsANullResponse() throws Exception {
    HiveConnectionFactory hmsConnection = Mockito.mock(HiveConnectionFactory.class);
    HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);

    Mockito.when(hmsConnection.connect()).thenReturn(new HMSClient(hmsClient));

    try (HiveNotificationFetcher fetcher = new HiveNotificationFetcher(hmsConnection)) {
      List<NotificationEvent> events;

      Mockito.when(hmsClient.getNextNotification(0, Integer.MAX_VALUE, null))
          .thenReturn(null);

      events = fetcher.fetchNotifications(0);
      assertTrue(events.isEmpty());
    }
  }

  @Test
  public void testGetEmptyNotificationsWhenHmsReturnsEmptyEvents() throws Exception {
    HiveConnectionFactory hmsConnection = Mockito.mock(HiveConnectionFactory.class);
    HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);

    Mockito.when(hmsConnection.connect()).thenReturn(new HMSClient(hmsClient));

    try (HiveNotificationFetcher fetcher = new HiveNotificationFetcher(hmsConnection)) {
      List<NotificationEvent> events;

      Mockito.when(hmsClient.getNextNotification(0, Integer.MAX_VALUE, null))
          .thenReturn(new NotificationEventResponse(Collections.<NotificationEvent>emptyList()));

      events = fetcher.fetchNotifications(0);
      assertTrue(events.isEmpty());
    }
  }

  @Test
  public void testGetAllNotificationsReturnedByHms() throws Exception {
    HiveConnectionFactory hmsConnection = Mockito.mock(HiveConnectionFactory.class);
    HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);

    Mockito.when(hmsConnection.connect()).thenReturn(new HMSClient(hmsClient));

    try (HiveNotificationFetcher fetcher = new HiveNotificationFetcher(hmsConnection)) {
      List<NotificationEvent> events;

      Mockito.when(hmsClient.getNextNotification(0, Integer.MAX_VALUE, null))
          .thenReturn(new NotificationEventResponse(
              Arrays.<NotificationEvent>asList(
                  new NotificationEvent(1L, 0, "CREATE_DATABASE", ""),
                  new NotificationEvent(2L, 0, "CREATE_TABLE", "")
              )
          ));

      events = fetcher.fetchNotifications(0);
      assertEquals(2, events.size());
      assertEquals(1, events.get(0).getEventId());
      assertEquals("CREATE_DATABASE", events.get(0).getEventType());
      assertEquals(2, events.get(1).getEventId());
      assertEquals("CREATE_TABLE", events.get(1).getEventType());
    }
  }
}

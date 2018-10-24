/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package org.apache.sentry.provider.db.service.persistent;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.sentry.binding.metastore.messaging.json.SentryJSONMessageFactory;

/**
 * Utilities for creating HMS NotificationEvent objects using in unit tests.
 */
class NotificationEventTestUtils {

  /**
   * A builder class that help create different types of NotificationEvent objects.
   */
  public static abstract class EventBuilder {
    static final SentryJSONMessageFactory messageFactory = new SentryJSONMessageFactory();
    static final int NO_TIME = 0;

    private final int eventId;
    private final EventType eventType;

    private EventBuilder(int eventId, EventType eventType) {
      this.eventId = eventId;
      this.eventType = eventType;
    }

    public NotificationEvent build() {
      return new NotificationEvent(eventId, NO_TIME, eventType.toString(), message().toString());
    }

    // Implement this method on each child class to return the EventMessage of the type of event.
    abstract HCatEventMessage message();

    static CreateDatabaseEventBuilder newCreateDatabaseEvent(int eventId) {
      return new CreateDatabaseEventBuilder(eventId);
    }

    static CreateTableEventBuilder newCreateTableEvent(int eventId) {
      return new CreateTableEventBuilder(eventId);
    }

    static AlterDatabaseEventBuilder newAlterDatabaseEvent(int eventId) {
      return new AlterDatabaseEventBuilder(eventId);
    }

    static AlterTableEventBuilder newAlterTableEvent(int eventId) {
      return new AlterTableEventBuilder(eventId);
    }
  }

  public static class AlterDatabaseEventBuilder extends EventBuilder {
    private String oldDbName, newDbName;
    private String oldLocation, newLocation;
    private PrincipalType oldOwnerType, newOwnerType;
    private String oldOwnerName, newOwnerName;

    private AlterDatabaseEventBuilder(int eventId) {
      super(eventId, EventType.ALTER_DATABASE);
    }

    public AlterDatabaseEventBuilder oldDbName(String oldDbName) {
      this.oldDbName = oldDbName;
      return this;
    }

    public AlterDatabaseEventBuilder newDbName(String newDbName) {
      this.newDbName = newDbName;
      return this;
    }

    public AlterDatabaseEventBuilder oldLocation(String oldLocation) {
      this.oldLocation = oldLocation;
      return this;
    }

    public AlterDatabaseEventBuilder newLocation(String newLocation) {
      this.newLocation = newLocation;
      return this;
    }

    public AlterDatabaseEventBuilder oldOwnerType(PrincipalType oldOwnerType) {
      this.oldOwnerType = oldOwnerType;
      return this;
    }

    public AlterDatabaseEventBuilder newOwnerType(PrincipalType newOwnerType) {
      this.newOwnerType = newOwnerType;
      return this;
    }

    public AlterDatabaseEventBuilder oldOwnerName(String oldOwnerName) {
      this.oldOwnerName = oldOwnerName;
      return this;
    }

    public AlterDatabaseEventBuilder newOwnerName(String newOwnerName) {
      this.newOwnerName = newOwnerName;
      return this;
    }

    @Override
    HCatEventMessage message() {
      Database before = new Database(oldDbName, null, oldLocation, null);
      Database after = new Database(newDbName, null, newLocation, null);

      before.setOwnerType(oldOwnerType);
      before.setOwnerName(oldOwnerName);
      after.setOwnerType(newOwnerType);
      after.setOwnerName(newOwnerName);

      return messageFactory.buildAlterDatabaseMessage(before, after);
    }
  }

  public static class AlterTableEventBuilder extends EventBuilder {
    private String oldDbName, newDbName;
    private String oldTableType, newTableType;
    private String oldTableName, newTableName;
    private String oldLocation, newLocation;
    private PrincipalType oldOwnerType, newOwnerType;
    private String oldOwnerName, newOwnerName;

    private AlterTableEventBuilder(int eventId) {
      super(eventId, EventType.ALTER_TABLE);
    }

    public AlterTableEventBuilder oldDbName(String oldDbName) {
      this.oldDbName = oldDbName;
      return this;
    }

    public AlterTableEventBuilder newDbName(String newDbName) {
      this.newDbName = newDbName;
      return this;
    }

    public AlterTableEventBuilder oldTableType(String oldTableType) {
      this.oldTableType = oldTableType;
      return this;
    }

    public AlterTableEventBuilder newTableType(String newTableType) {
      this.newTableType = newTableType;
      return this;
    }

    public AlterTableEventBuilder oldTableName(String oldTableName) {
      this.oldTableName = oldTableName;
      return this;
    }

    public AlterTableEventBuilder newTableName(String newTableName) {
      this.newTableName = newTableName;
      return this;
    }

    public AlterTableEventBuilder oldLocation(String oldLocation) {
      this.oldLocation = oldLocation;
      return this;
    }

    public AlterTableEventBuilder newLocation(String newLocation) {
      this.newLocation = newLocation;
      return this;
    }

    public AlterTableEventBuilder oldOwnerType(PrincipalType oldOwnerType) {
      this.oldOwnerType = oldOwnerType;
      return this;
    }

    public AlterTableEventBuilder newOwnerType(PrincipalType newOwnerType) {
      this.newOwnerType = newOwnerType;
      return this;
    }

    public AlterTableEventBuilder oldOwnerName(String oldOwnerName) {
      this.oldOwnerName = oldOwnerName;
      return this;
    }

    public AlterTableEventBuilder newOwnerName(String newOwnerName) {
      this.newOwnerName = newOwnerName;
      return this;
    }

    @Override
    HCatEventMessage message() {
      StorageDescriptor oldSd = new StorageDescriptor();
      StorageDescriptor newSd = new StorageDescriptor();

      oldSd.setLocation(oldLocation);
      newSd.setLocation(newLocation);

      Table before =
        new Table(oldTableName, oldDbName, oldOwnerName, NO_TIME, NO_TIME, 0, oldSd, null, null, null, null, oldTableType);
      Table after =
        new Table(newTableName, newDbName, newOwnerName, NO_TIME, NO_TIME, 0, newSd, null, null, null, null, newTableType);

      before.setOwnerType(oldOwnerType);
      after.setOwnerType(newOwnerType);

      return messageFactory.buildAlterTableMessage(before, after);
    }
  }

  public static class CreateTableEventBuilder extends EventBuilder {
    private String dbName;
    private String tableType;
    private String tableName;
    private String location;
    private PrincipalType ownerType;
    private String ownerName;

    private CreateTableEventBuilder(int eventId) {
      super(eventId, EventType.CREATE_TABLE);
    }

    public CreateTableEventBuilder dbName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    public CreateTableEventBuilder tableType(String tableType) {
      this.tableType = tableType;
      return this;
    }

    public CreateTableEventBuilder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public CreateTableEventBuilder location(String location) {
      this.location = location;
      return this;
    }

    public CreateTableEventBuilder ownerType(PrincipalType ownerType) {
      this.ownerType = ownerType;
      return this;
    }

    public CreateTableEventBuilder ownerName(String ownerName) {
      this.ownerName = ownerName;
      return this;
    }

    @Override
    HCatEventMessage message() {
      StorageDescriptor sd = new StorageDescriptor();
      sd.setLocation(location);

      Table table =
        new Table(tableName, dbName, ownerName, NO_TIME, NO_TIME, 0, sd, null, null, null, null, tableType);

      table.setOwnerType(ownerType);

      return messageFactory.buildCreateTableMessage(table);
    }
  }

  public static class CreateDatabaseEventBuilder extends EventBuilder {
    private String dbName;
    private String location;
    private PrincipalType ownerType;
    private String ownerName;

    private CreateDatabaseEventBuilder(int eventId) {
      super(eventId, EventType.CREATE_DATABASE);
    }

    public CreateDatabaseEventBuilder dbName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    public CreateDatabaseEventBuilder location(String location) {
      this.location = location;
      return this;
    }

    public CreateDatabaseEventBuilder ownerType(PrincipalType ownerType) {
      this.ownerType = ownerType;
      return this;
    }

    public CreateDatabaseEventBuilder ownerName(String ownerName) {
      this.ownerName = ownerName;
      return this;
    }

    @Override
    HCatEventMessage message() {
      Database db = new Database(dbName, null, location, null);
      db.setOwnerType(ownerType);
      db.setOwnerName(ownerName);

      return messageFactory.buildCreateDatabaseMessage(db);
    }
  }
}

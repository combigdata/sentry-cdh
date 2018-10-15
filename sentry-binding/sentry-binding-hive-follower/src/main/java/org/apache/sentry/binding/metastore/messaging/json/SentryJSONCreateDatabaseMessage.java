/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.binding.metastore.messaging.json;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hive.hcatalog.messaging.json.JSONCreateDatabaseMessage;
import org.codehaus.jackson.annotate.JsonProperty;

public class SentryJSONCreateDatabaseMessage extends JSONCreateDatabaseMessage {
    @JsonProperty private String location;
    @JsonProperty private PrincipalType ownerType;
    @JsonProperty private String ownerName;

  public SentryJSONCreateDatabaseMessage() {
  }

  public SentryJSONCreateDatabaseMessage(String server, String servicePrincipal, String db, Long timestamp, String location) {
    super(server, servicePrincipal, db, timestamp);
    this.location = location;
  }

    public SentryJSONCreateDatabaseMessage(String server, String servicePrincipal, Long timestamp, Database db) {
        this(server, servicePrincipal, db.getName(), timestamp, db.getLocationUri());
        this.ownerType = db.getOwnerType();
        this.ownerName = db.getOwnerName();
    }

    public String getLocation() {
        return location;
    }

    public PrincipalType getOwnerType() {
        return ownerType;
    }

    public String getOwnerName() {
        return ownerName;
    }

    @Override
    public String toString() {
        return SentryJSONMessageDeserializer.serialize(this);
    }
}

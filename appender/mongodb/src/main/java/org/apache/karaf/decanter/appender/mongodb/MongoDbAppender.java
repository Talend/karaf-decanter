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
package org.apache.karaf.decanter.appender.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.karaf.decanter.api.marshaller.Marshaller;
import org.apache.karaf.decanter.appender.utils.EventFilter;
import org.bson.Document;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.*;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventConstants;
import org.osgi.service.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Dictionary;

@Component(
    name = "org.apache.karaf.decanter.appender.mongodb",
    immediate = true,
    configurationPolicy = ConfigurationPolicy.REQUIRE,
    property = EventConstants.EVENT_TOPIC + "=decanter/collect/*"
)
public class MongoDbAppender implements EventHandler {

    public static String URI_PROPERTY = "uri";
    public static String DATABASE_PROPERTY = "database";
    public static String COLLECTION_PROPERTY = "collection";

    public static String URI_DEFAULT = "mongodb://localhost";
    public static String DATABASE_DEFAULT = "decanter";
    public static String COLLECTION_DEFAULT = "decanter";

    @Reference
    public Marshaller marshaller;

    private final static Logger LOGGER = LoggerFactory.getLogger(MongoDbAppender.class);

    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection mongoCollection;

    private Dictionary<String, Object> config;

    @Activate
    public void activate(ComponentContext componentContext) {
        config = componentContext.getProperties();

        String uri = getValue(config, URI_PROPERTY, URI_DEFAULT);
        String database = getValue(config, DATABASE_PROPERTY, DATABASE_DEFAULT);
        String collection = getValue(config, COLLECTION_PROPERTY, COLLECTION_DEFAULT);

        mongoClient = new MongoClient(new MongoClientURI(uri));
        mongoDatabase = mongoClient.getDatabase(database);
        mongoCollection = mongoDatabase.getCollection(collection);
    }

    private String getValue(Dictionary<String, Object> config, String key, String defaultValue) {
        String value = (String)config.get(key);
        return (value != null) ? value :  defaultValue;
    }

    @Override
    public void handleEvent(Event event) {
        if (EventFilter.match(event, config)) {
            try {
                String data = marshaller.marshal(event);
                mongoCollection.insertOne(Document.parse(data));
            } catch (Exception e) {
                LOGGER.warn("Error storing event in MongoDB", e);
            }
        }
    }

    @Deactivate
    public void deactivate(ComponentContext componentContext) {
        mongoClient.close();
    }

}

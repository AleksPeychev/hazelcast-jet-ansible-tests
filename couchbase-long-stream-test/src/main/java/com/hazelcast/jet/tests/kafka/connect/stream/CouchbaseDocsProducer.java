/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.tests.kafka.connect.stream;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.hazelcast.logging.ILogger;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;

public class CouchbaseDocsProducer {
    private static final int PRINT_LOG_INSERT_ITEMS = 5_000;
    private final Bucket bucket;
    private final String collectionName;
    private final ILogger logger;
    private final Thread producerThread;
    private volatile boolean running = true;
    private volatile long producedItems;

    //must add data for infinite time to Couchbase
    //only inserting data need to be adjusted
    public CouchbaseDocsProducer(final Bucket bucket, final String collectionName, ILogger logger) {
        this.bucket = bucket;
        this.collectionName = collectionName;
        this.logger = logger;
        this.producerThread = new Thread(() -> uncheckRun(this::run));
    }

    private void run() {
        Collection collection = bucket.collection(collectionName);
        long id = 0;
        while (running) {
            collection.insert(String.valueOf(id), JsonObject.create().put("docId", id++));
            producedItems = id;

            if (id % PRINT_LOG_INSERT_ITEMS == 0) {
                logger.info(String.format("Inserted %d docs into %s collection)", id, collectionName));
            }
            sleepMillis(150);
        }
        logger.info(
                String.format("Total number of inserted docs into %s collection is %d", collectionName, producedItems));
    }

    public void start() {
        producerThread.start();
    }

    public long stop()
            throws InterruptedException {
        running = false;
        producerThread.join();
        return producedItems;
    }

}

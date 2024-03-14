/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.tests.couchbase;

import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.bucket.BucketManager;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.couchbase.client.java.manager.bucket.BucketType;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.kafka.connect.KafkaConnectSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.shaded.com.fasterxml.jackson.jr.ob.impl.DeferredMap;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Base64;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.IntStream;

import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;

public class CouchbaseTest
        extends AbstractSoakTest {
    private static final int DEFAULT_ITEM_COUNT = 10;
    private static final int LOG_JOB_COUNT_THRESHOLD = 50;
    private static final int SLEEP_BETWEEN_READS_SECONDS = 2;
    private static final int JOB_STATUS_ASSERTION_ATTEMPTS = 1200;
    private static final int JOB_STATUS_ASSERTION_SLEEP_MS = 100;
    private static final int STREAM_SINK_ASSERTION_ATTEMPTS = 120;
    private static final int STREAM_SINK_ASSERTION_SLEEP_MS = 1000;
    private static final String COLLECTION_PREFIX = "collection_";
    private static final String DOC_PREFIX = "couchbase-document-from-collection-";
    private static final String DOC_COUNTER_PREFIX = "-counter-";
    private static final String STREAM_READ_FROM_PREFIX = CouchbaseTest.class.getSimpleName() + "_streamReadFrom_";
    private static final String STREAM_SINK_LIST_NAME = CouchbaseTest.class.getSimpleName() + "_listSinkStream";
    private static final String BUCKET_NAME = CouchbaseTest.class.getSimpleName();
    private static Bucket bucket;
    private String couchbaseConnectionString;
    private String couchbaseUsername;
    private String couchbasePwd;
    private int itemCount;

    public static void main(final String[] args) throws Exception {
        setRunLocal();
        new CouchbaseTest().run(args);
    }

    @Override
    public void init(final HazelcastInstance client) {
        couchbaseConnectionString = "couchbase://" + property("couchbaseIp", "127.0.0.1") + ":11210";
        couchbaseUsername = "Administrator";
        couchbasePwd = "Soak-test,1";
        ClusterEnvironment environment = ClusterEnvironment.builder().retryStrategy(BestEffortRetryStrategy.INSTANCE)
                                                           .timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofMillis(2500)))
                                                           .build();
        Cluster cluster = Cluster.connect(couchbaseConnectionString,
                clusterOptions(couchbaseUsername, couchbasePwd).environment(environment));
        itemCount = propertyInt("itemCount", DEFAULT_ITEM_COUNT);

        BucketManager bucketManager = cluster.buckets();
        BucketSettings bucketSettings = BucketSettings.create(BUCKET_NAME).bucketType(BucketType.COUCHBASE)
                                                      // RAM quota for the bucket is mandatory set it to 6GB as c5.xlarge instance has 8GB
                                                      .ramQuotaMB(propertyInt("couchbaseRamQuotaMb", 2048)).numReplicas(0)
                                                      .replicaIndexes(false).flushEnabled(true);
        bucketManager.createBucket(bucketSettings);

        bucket = cluster.bucket(BUCKET_NAME);
        bucket.waitUntilReady(Duration.ofSeconds(10));
    }

    @Override
    public void test(final HazelcastInstance client, final String name) {
        clearSinks(client);
        int jobCounter = 0;
        final long begin = System.currentTimeMillis();
        try {
            while (System.currentTimeMillis() - begin < durationInMillis) {
                deleteCollectionAndCreateNewOne(jobCounter);
                clearSinks(client);

                startStreamReadFromCouchbasePipeline(client, jobCounter);
                createNumberOfRecords(jobCounter, itemCount);
                assertStreamResults(client, jobCounter);

                stopStreamRead(client, jobCounter);
                clearSinks(client);
                deleteCollectionAndCreateNewOne(jobCounter);

                if (jobCounter % LOG_JOB_COUNT_THRESHOLD == 0) {
                    logger.info("Job count: " + jobCounter);
                }

                jobCounter++;
                sleepSeconds(SLEEP_BETWEEN_READS_SECONDS);
            }
        } finally {
            logger.info("Test finished with job count: " + jobCounter);
        }
    }

    @Override
    protected void teardown(final Throwable t) {
    }

    private static String recordValue(final int collectionCounter, final int docCounter) {
        return DOC_PREFIX + collectionCounter + DOC_COUNTER_PREFIX + docCounter;
    }

    private static void stopStreamRead(final HazelcastInstance client, final int collectionCounter) {
        client.getJet().getJob(STREAM_READ_FROM_PREFIX + collectionCounter).cancel();
    }

    private static void clearSinks(final HazelcastInstance client) {
        client.getList(STREAM_SINK_LIST_NAME).clear();
    }

    private static void assertJobStatusEventually(final Job job) {
        for (int i = 0; i < JOB_STATUS_ASSERTION_ATTEMPTS; i++) {
            if (job.getStatus().equals(RUNNING)) {
                return;
            } else {
                sleepMillis(JOB_STATUS_ASSERTION_SLEEP_MS);
            }
        }
        throw new AssertionError(
                "Job " + job.getName() + " does not have expected status: " + RUNNING + ". Job status: " + job.getStatus());
    }

    private void deleteCollectionAndCreateNewOne(final int collectionCounter) {
        CollectionManager collectionMgr = bucket.collections();
        try {
            collectionMgr.dropCollection(CollectionSpec.create(COLLECTION_PREFIX + collectionCounter));
            System.out.println("Collection deleted successfully");
        } catch (CollectionNotFoundException e) {
            System.out.println("Collection not found: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error deleting collection: " + e.getMessage());
        }

        collectionMgr.createCollection(CollectionSpec.create(COLLECTION_PREFIX + collectionCounter));
    }

    private void startStreamReadFromCouchbasePipeline(final HazelcastInstance client, final int collectionCounter) {
        final StreamSource<String> couchbaseSource = streamSource(collectionCounter);
        final Pipeline fromCouchbase = Pipeline.create();
        fromCouchbase.readFrom(couchbaseSource).withoutTimestamps().setLocalParallelism(1)
                     .map(base64 -> Base64.getDecoder().decode(base64)).map(JsonUtil::mapFrom)
                     .map(entry -> ((DeferredMap) entry.get("content")).get("docId").toString())
                     .writeTo(Sinks.list(STREAM_SINK_LIST_NAME));

        final JobConfig jobConfig = new JobConfig().addClass(TestUtil.class, com.fasterxml.jackson.core.JsonFactory.class)
                                                   .addJarsInZip(Paths.get(
                                                                              "/Users/apeychev/proj/couchbase-kafka-connect-couchbase-4.1.11.zip")
                                                                      .toFile());
        jobConfig.setName(STREAM_READ_FROM_PREFIX + collectionCounter);
        final Job job = client.getJet().newJob(fromCouchbase, jobConfig);
        assertJobStatusEventually(job);

    }

    private void assertStreamResults(final HazelcastInstance client, final int collectionCounter) {
        for (int i = 0; i < STREAM_SINK_ASSERTION_ATTEMPTS; i++) {
            long actualTotalCount = client.getList(STREAM_SINK_LIST_NAME).size();
            if (itemCount == actualTotalCount) {
                break;
            }
            sleepMillis(STREAM_SINK_ASSERTION_SLEEP_MS);
        }

        assertResults(client.getList(STREAM_SINK_LIST_NAME), collectionCounter);
    }

    public StreamSource<String> streamSource(int collectionCounter) {
        Properties properties = new Properties();
        properties.setProperty("name", "couchbase");
        properties.setProperty("connector.class", "com.couchbase.connect.kafka.CouchbaseSourceConnector");
        properties.setProperty("couchbase.bucket", BUCKET_NAME);
        properties.setProperty("couchbase.seed.nodes", couchbaseConnectionString);
        properties.setProperty("couchbase.password", couchbasePwd);
        properties.setProperty("couchbase.username", couchbaseUsername);
        properties.setProperty("couchbase.collections", "_default." + COLLECTION_PREFIX + collectionCounter);
        properties.setProperty("couchbase.source.handler",
                "com.couchbase.connect.kafka.handler.source.RawJsonWithMetadataSourceHandler");
        return KafkaConnectSources.connect(properties, TestUtil::convertToString);
    }

    private void assertResults(final IList<String> list, final int collectionCounter) {
        final Set<String> set = new HashSet<>();
        final String expected = DOC_PREFIX + collectionCounter + DOC_COUNTER_PREFIX;
        for (final String item : list) {
            assertTrue("List does not contain expected part: " + item, item.contains(expected));
            set.add(item);
        }

        final int lastElementNumber = itemCount - 1;
        try {
            assertEquals(itemCount, list.size());
            assertEquals(itemCount, set.size());
            assertTrue(set.contains(recordValue(collectionCounter, 0)));
            assertTrue(set.contains(recordValue(collectionCounter, lastElementNumber)));
        } catch (final Throwable ex) {
            logger.info("Printing content of incorrect list for list" + ":");
            for (final String item : list) {
                logger.info(item);
            }
            throw ex;
        }
    }

    private void createNumberOfRecords(int collectionCounter, int expectedSize) {
        IntStream.range(0, expectedSize).forEach(value -> {
            createOneRecord(collectionCounter, value);
        });
    }

    public void createOneRecord(int collectionCounter, long index) {
        Collection collection = bucket.collection(COLLECTION_PREFIX + collectionCounter);
        collection.insert(String.valueOf(index),
                JsonObject.create().put("docId", DOC_PREFIX + collectionCounter + DOC_COUNTER_PREFIX + index));
    }
}

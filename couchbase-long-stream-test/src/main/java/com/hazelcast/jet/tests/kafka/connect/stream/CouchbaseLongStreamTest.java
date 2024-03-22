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

package com.hazelcast.jet.tests.kafka.connect.stream;

import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.bucket.BucketManager;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.couchbase.client.java.manager.bucket.BucketType;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.kafka.connect.KafkaConnectSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.logging.ILogger;
import com.hazelcast.shaded.com.fasterxml.jackson.jr.ob.impl.DeferredMap;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;

public class CouchbaseLongStreamTest
        extends AbstractSoakTest {
    private static final int ASSERTION_RETRY_COUNT = 60;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int DEFAULT_TIMEOUT_FOR_NO_DATA_PROCESSED_MIN = 5;
    private static final int ASSERTION_ATTEMPTS = 1200;
    private static final int ASSERTION_SLEEP_MS = 100;
    private static final String BUCKET_NAME = CouchbaseLongStreamTest.class.getSimpleName();
    private static final String CONNECTOR_URL = "https://repository.hazelcast.com/download"
            + "/tests/couchbase-kafka-connect-couchbase-4.1.11.zip";
    private String couchbaseConnectionString;
    private String couchbaseUsername;
    private String couchbasePassword;
    private int snapshotIntervalMs;
    private int timeoutForNoDataProcessedMin;
    private Cluster cluster;
    private Bucket bucket;

    public static void main(final String[] args)
            throws Exception {
        new CouchbaseLongStreamTest().run(args);
    }

    @Override
    public void init(final HazelcastInstance client) {
        couchbaseConnectionString = "couchbase://" + property("couchbaseIp", "34.229.174.151") + ":11210";
        couchbaseUsername = "Administrator";
        couchbasePassword = "Soak-test,1";
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        //don`t see it as argument in Ansible tests
        timeoutForNoDataProcessedMin = propertyInt("timeoutForNoProcessedDataMin",
                DEFAULT_TIMEOUT_FOR_NO_DATA_PROCESSED_MIN);
        //don`t see here the argument form Ansible tests : remoteClusterYaml={{jet_home}}/config/hazelcast-client.yaml
        cluster = Cluster.connect(couchbaseConnectionString, couchbaseUsername, couchbasePassword);
        BucketManager bucketManager = cluster.buckets();
        // RAM quota for the bucket is mandatory set it to 6GB as c5.xlarge instance has 8GB
        BucketSettings bucketSettings = BucketSettings.create(BUCKET_NAME)
                                                      .bucketType(BucketType.COUCHBASE)
                                                      .ramQuotaMB(propertyInt("couchbaseRamQuotaMb", 2048))
                                                      .numReplicas(1)
                                                      .replicaIndexes(true)
                                                      .flushEnabled(false).maxExpiry(Duration.ZERO);
        bucketManager.createBucket(bucketSettings);

        bucket = cluster.bucket(BUCKET_NAME);
        bucket.waitUntilReady(Duration.ofSeconds(10));
    }

    //Need it for the Dynamic_cluster
    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    //one new bucket for both clusters and not use _default and each cluster should use different collection
    //database is bucket(test name couchbaselongstreamtest) and collection can be cluster name
    @Override
    public void test(final HazelcastInstance client, final String clusterName)
            throws Exception {
        final long begin = System.currentTimeMillis();

        deleteCollectionAndCreateNewOne(clusterName);

        final StreamSource<String> couchbaseSource = streamSource(clusterName);

        //map object to long, because verification processor needs long
        final Pipeline fromCouchbase = Pipeline.create();
        fromCouchbase.readFrom(couchbaseSource).withoutTimestamps().map(base64 -> Base64.getDecoder().decode(base64))
                     .map(JsonUtil::mapFrom).map(entry -> (Long) ((DeferredMap) entry.get("content")).get("docId"))
                     .writeTo(VerificationProcessor.sink(clusterName));

        //add the classes and coucnhbase client jar
        final JobConfig jobConfig = new JobConfig();

        if (clusterName.startsWith(DYNAMIC_CLUSTER)) {
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(EXACTLY_ONCE);
        } else {
            jobConfig.addClass(TestUtil.class, com.fasterxml.jackson.core.JsonFactory.class)
                     .addJarsInZip(getCouchbaseConnectorURL());
        }

        jobConfig.setName(clusterName + "_" + BUCKET_NAME);

        final Job job = client.getJet().newJob(fromCouchbase, jobConfig);
        assertJobStatusEventually(job);

        final CouchbaseDocsProducer producer = new CouchbaseDocsProducer(couchbaseConnectionString, couchbaseUsername,
                couchbasePassword, BUCKET_NAME, clusterName, logger);
        producer.start();

        final long expectedTotalCount;
        long lastlyProcessed = -1;
        int noNewDocsCounter = 0;
        try {
            while (System.currentTimeMillis() - begin < durationInMillis) {
                if (getJobStatusWithRetry(job) == FAILED) {
                    job.join();
                } else {
                    //check that smth is processed
                    final long processedDocs = getNumberOfProcessedDocs(client, clusterName);

                    if (processedDocs == lastlyProcessed) {
                        noNewDocsCounter++;
                        log(logger, "Nothing was processed in last minute, current counter:" + processedDocs,
                                clusterName);
                        if (noNewDocsCounter > timeoutForNoDataProcessedMin) {
                            throw new AssertionError("Failed. Exceeded timeout for no data processed");
                        }
                    } else {
                        noNewDocsCounter = 0;
                        lastlyProcessed = processedDocs;
                    }
                }
                sleepMinutes(1);
            }
        } finally {
            expectedTotalCount = producer.stop();
        }

        log(logger, "Producer stopped, expectedTotalCount: " + expectedTotalCount, clusterName);
        //Final validation
        assertCountEventually(client, expectedTotalCount, clusterName);
        job.cancel();
        log(logger, "Job completed", clusterName);

    }

    private static void assertJobStatusEventually(final Job job) {
        for (int i = 0; i < ASSERTION_ATTEMPTS; i++) {
            if (job.getStatus().equals(RUNNING)) {
                return;
            } else {
                sleepMillis(ASSERTION_SLEEP_MS);
            }
        }
        throw new AssertionError(
                "Job " + job.getName() + " does not have expected status: " + RUNNING + ". Job status: "
                        + job.getStatus());
    }

    private static long getNumberOfProcessedDocs(final HazelcastInstance client, final String clusterName) {
        final Map<String, Long> latestCounterMap = client.getMap(VerificationProcessor.CONSUMED_DOCS_MAP_NAME);
        return Optional.ofNullable(latestCounterMap.get(clusterName)).orElse(0L);
    }

    private static void assertCountEventually(final HazelcastInstance client, final long expectedTotalCount,
                                              final String clusterName) {
        final Map<String, Long> latestCounterMap = client.getMap(VerificationProcessor.CONSUMED_DOCS_MAP_NAME);
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            final long actualTotalCount = latestCounterMap.get(clusterName);
            if (expectedTotalCount == actualTotalCount) {
                return;
            }
            sleepSeconds(1);
        }
        final long actualTotalCount = latestCounterMap.get(clusterName);
        assertEquals(expectedTotalCount, actualTotalCount);
    }

    private static void log(final ILogger logger, final String message, final String clusterName) {
        logger.info("Cluster" + clusterName + "\t\t" + message);
    }

    private void deleteCollectionAndCreateNewOne(final String collectionName) {
        CollectionManager collectionMgr = bucket.collections();
        try {
            collectionMgr.dropCollection(CollectionSpec.create(collectionName));
            System.out.println("Collection deleted successfully");
        } catch (CollectionNotFoundException e) {
            System.out.println("Collection not found: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error deleting collection: " + e.getMessage());
        }

        collectionMgr.createCollection(CollectionSpec.create(collectionName, Duration.ZERO));
    }

    public StreamSource<String> streamSource(String clusterName) {
        Properties properties = new Properties();
        properties.setProperty("name", "couchbase");
        properties.setProperty("connector.class", "com.couchbase.connect.kafka.CouchbaseSourceConnector");
        properties.setProperty("couchbase.bucket", BUCKET_NAME);
        properties.setProperty("couchbase.seed.nodes", couchbaseConnectionString);
        properties.setProperty("couchbase.password", "Soak-test,1");
        properties.setProperty("couchbase.username", "Administrator");
        properties.setProperty("couchbase.collections", "_default." + clusterName);
        properties.setProperty("couchbase.source.handler",
                "com.couchbase.connect.kafka.handler.source.RawJsonWithMetadataSourceHandler");
        return KafkaConnectSources.connect(properties, TestUtil::convertToString);
    }

    private URL getCouchbaseConnectorURL() {
        try {
            return new URL(CONNECTOR_URL);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    protected void teardown(final Throwable t) {
    }
}

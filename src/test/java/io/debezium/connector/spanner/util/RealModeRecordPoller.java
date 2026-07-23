/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RealModeRecordPoller {

    private static final Logger LOG = LoggerFactory.getLogger(RealModeRecordPoller.class);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(250);
    private static final Map<String, ?> EMPTY_OFFSET_MAP = Collections.emptyMap();

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final JsonConverter keyConverter;
    private final JsonConverter valueConverter;
    private final BlockingQueue<SourceRecord> destination;
    private final Pattern subscribedTopics;

    private volatile boolean shouldRun;
    private Thread pollerThread;

    public RealModeRecordPoller(String kafkaBootstrapServers, Pattern subscribedTopics, BlockingQueue<SourceRecord> destination) {
        this.subscribedTopics = subscribedTopics;
        this.destination = destination;
        this.consumer = buildConsumer(kafkaBootstrapServers);
        this.keyConverter = buildJsonConverter(true);
        this.valueConverter = buildJsonConverter(false);
    }

    private static KafkaConsumer<byte[], byte[]> buildConsumer(String bootstrapServers) {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "spanner-real-mode-verification-" + UUID.randomUUID());
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaConsumer<>(consumerProps);
    }

    private static JsonConverter buildJsonConverter(boolean forKeys) {
        JsonConverter jsonConverter = new JsonConverter();
        Map<String, Object> converterProps = new HashMap<>();
        converterProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, Boolean.TRUE);
        jsonConverter.configure(converterProps, forKeys);
        return jsonConverter;
    }

    public void start() {
        shouldRun = true;
        consumer.subscribe(subscribedTopics);
        pollerThread = new Thread(this::runPollLoop, "spanner-real-mode-record-poller");
        pollerThread.setDaemon(true);
        pollerThread.start();
    }

    private void runPollLoop() {
        while (shouldRun) {
            try {
                drainOnePoll();
            }
            catch (WakeupException wakeup) {
                LOG.debug("Real-mode record poller woken up, checking shutdown flag");
            }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
            catch (RuntimeException unexpected) {
                LOG.warn("Unexpected error while polling real-mode Kafka Connect output topics", unexpected);
            }
        }
    }

    private void drainOnePoll() throws InterruptedException {
        ConsumerRecords<byte[], byte[]> polled = consumer.poll(POLL_INTERVAL);
        for (ConsumerRecord<byte[], byte[]> polledRecord : polled) {
            destination.put(convertToSourceRecord(polledRecord));
        }
    }

    private SourceRecord convertToSourceRecord(ConsumerRecord<byte[], byte[]> polledRecord) {
        SchemaAndValue decodedKey = keyConverter.toConnectData(polledRecord.topic(), polledRecord.key());
        SchemaAndValue decodedValue = valueConverter.toConnectData(polledRecord.topic(), polledRecord.value());
        return new SourceRecord(
                EMPTY_OFFSET_MAP,
                EMPTY_OFFSET_MAP,
                polledRecord.topic(),
                polledRecord.partition(),
                decodedKey.schema(),
                decodedKey.value(),
                decodedValue.schema(),
                decodedValue.value());
    }

    public void stop() {
        shouldRun = false;
        consumer.wakeup();
        joinPollerThread();
        consumer.close();
    }

    private void joinPollerThread() {
        if (pollerThread == null) {
            return;
        }
        try {
            pollerThread.join(TimeUnit.SECONDS.toMillis(30));
        }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}

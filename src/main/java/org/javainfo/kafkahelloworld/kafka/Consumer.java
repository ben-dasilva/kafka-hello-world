package org.javainfo.kafkahelloworld.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class Consumer {
    private static final long POLL_TIMEOUT = 10_000;

    public void consume() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties());

        consumer.subscribe(singletonList("devnation"));

        for (int i = 0; i < 20; i++) {
            ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);

            if (records.isEmpty()) {
                System.out.println("No records to consume.");
                return;
            }

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received message: " + record);
            }
        }
    }

    private Properties kafkaProperties() {
        Properties p = new Properties();

        String[][] pVals = {
                {BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"},
                {GROUP_ID_CONFIG, "devnation-java"},
                {ENABLE_AUTO_COMMIT_CONFIG, "true"},
                {AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"},
                {SESSION_TIMEOUT_MS_CONFIG, "30000"},
                {KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"},
                {VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"},
                {AUTO_OFFSET_RESET_CONFIG, "earliest"},
        };

        p.putAll(Arrays.stream(pVals).collect(toMap(e -> e[0], e -> e[1])));

        return p;
    }
}

package org.javainfo.kafkahelloworld.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class Producer {
    private static final long POLL_TIMEOUT = 10_000;

    public void produce() {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties())) {
            for (int i = 0; i < 1; i++) {
                Future<RecordMetadata> sending = producer.send(createRecord());
                RecordMetadata recordMetadata = sending.get();

                System.out.println("recordMetadata = " + recordMetadata);

                Thread.sleep(1_000);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private ProducerRecord<String, String> createRecord() {
        return new ProducerRecord<>("devnation",
                "Hello World from Java Producer! (" + LocalDateTime.now() + ")");
    }

    private Properties kafkaProperties() {
        Properties p = new Properties();

        String[][] pVals = {
                {BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"},
                {KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"},
                {VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"},
        };

        p.putAll(Arrays.stream(pVals).collect(toMap(e -> e[0], e -> e[1])));

        return p;
    }
}

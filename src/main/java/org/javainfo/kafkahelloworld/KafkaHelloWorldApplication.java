package org.javainfo.kafkahelloworld;

import org.javainfo.kafkahelloworld.kafka.Consumer;
import org.javainfo.kafkahelloworld.kafka.Producer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaHelloWorldApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaHelloWorldApplication.class, args);

        new Producer().produce();
        new Consumer().consume();
    }
}


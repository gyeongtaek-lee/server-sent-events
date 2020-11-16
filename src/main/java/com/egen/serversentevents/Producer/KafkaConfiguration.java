package com.egen.serversentevents.Producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.annotation.EnableKafka;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfiguration {

    private static final String TOPIC = "egen-sse-test-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID_CONFIG = "trans-string-consumer-egen-new";

    private KafkaSender<String, String> sender = null;   // Mono<Producer>

    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }

    @Bean
    public KafkaSender kafkaSender() {

        SenderOptions<String, String> senderOptions = SenderOptions.create(producerConfigs());
        sender = KafkaSender.create(senderOptions);

        return sender;

    }

    public void close() {
        sender.close();
    }

}

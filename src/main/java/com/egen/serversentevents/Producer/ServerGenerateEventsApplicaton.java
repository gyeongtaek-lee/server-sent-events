package com.egen.serversentevents.Producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * StringSerializer를 사용하여 메세지를 생산하는 Producer Class
 */
public class ServerGenerateEventsApplicaton {

    private static final Logger log = LoggerFactory.getLogger(ServerGenerateEventsApplicaton.class.getName());

    private static final String TOPIC = "egen-sse-test-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID_CONFIG = "trans-string-consumer-egen-new";

    private final KafkaSender<String, String> sender;   // Mono<Producer>
    private final SimpleDateFormat dateFormat;

    public ServerGenerateEventsApplicaton(String bootstrapServers) {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        SenderOptions<String, String> senderOptions = SenderOptions.create(props);
        sender = KafkaSender.create(senderOptions);

        dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");

    }

    public void close() {
        sender.close();
    }

    /**
     * 10초 간격으로 subscribe 을 수행하여 SenderRecord를 생성한 후 kafka의 topic에 push
     * @param topic
     * @param count
     * @param latch
     * @throws InterruptedException
     */
    public void generateMessages(String topic, int count, CountDownLatch latch) throws InterruptedException {

        // public <T> Flux<SenderResult<T>> send(Publisher<? extends SenderRecord<K, V, T>> records)
        // SenderRecord를 1부터 20까지 생성
        // SenderRecord는 ProducerRecord를 매개변수로 생성
        // 바로 subscribe 하여 reactor를 수행
       sender.<Integer>send(Flux.range(1, count)
                .map(i -> SenderRecord.create(new ProducerRecord<>(topic, "Key_"+i, "Message 11ST _ "+i), i)))
                .doOnError(e -> log.error("Send failed", e))
                .subscribe(r -> {
                    RecordMetadata metadata = r.recordMetadata();   // SenderRecord의 메타 데이터

                    System.out.printf("Message %d sent successfully, topic-partition=%s-%d offset=%d timestamp=%s%n", r.correlationMetadata(), metadata.topic(), metadata.partition(), metadata.offset(), dateFormat.format(new Date(metadata.timestamp())));

                    latch.countDown();
                });
    }

    public static void main(String[] args) throws InterruptedException {

        int count = 20;

        CountDownLatch latch = new CountDownLatch(count);

        ServerGenerateEventsApplicaton producer = new ServerGenerateEventsApplicaton(BOOTSTRAP_SERVERS);
        producer.generateMessages(TOPIC, count, latch);
        latch.await(10, TimeUnit.SECONDS);
        producer.close();

    }

}

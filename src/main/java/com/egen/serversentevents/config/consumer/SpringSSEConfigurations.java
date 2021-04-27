package com.egen.serversentevents.config.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Consumer 인 Kafka Receiver Configuration Class
 */
@Configuration
public class SpringSSEConfigurations {

    private KafkaReceiver<Object, Object> receiver = null;   // Mono<Producer>

    private static final String TOPIC = "egen-sse-test-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CLIENT_ID_CONFIG = "trans-string-consumer-egen-new";
    private static final String GROUP_ID_CONFIG = "trans-string-cg-egen-new";

    @Bean
    public KafkaReceiver kafkaReceiver() {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 가장 빠른 메시지 read (첫 offset 부터...)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);  // 자동 커밋 활성화

        // subscription(Collection:topics). 또 다른 방법. @KafkaListener annotation 사용 하여 message receive
        // consumer는 event listener 처럼 message queue에 데이터가 들어오면 처리된다.
        /**
         * ReceiverOptions.subscription() 에 토픽 컬렉션을 넘기면 같은 API로 둘 이상의 토픽을 구독할 수 있다.
         * 구독할 패턴을 지정하는 식으로 와일드카드 패턴도 구독할 수 있다.
         * kafakaConsumer 는 그룹 관리를 통해 패턴과 매칭되는 토픽이 만들어지거나 삭제 될 때 동적으로 할당된 토픽을 업데이트하며, 매칭한 토픽의 파티션을 사용 가능한 컨슈머 인스턴스에 할당한다.
         * receiverOptions = receiverOptions.subscription(Pattern.compile("demo.*")); // "demo"로 시작하는 모든 토픽 레코드를 컨슘한다.
         */
        receiver = KafkaReceiver.create(ReceiverOptions.create(props).subscription(Collections.singleton(TOPIC)));

        return receiver;

//        return new DefaultKafkaReceiver(ConsumerFactory.INSTANCE, ReceiverOptions.create(props).subscription(Collections.singleton(TOPIC)));

    }

}
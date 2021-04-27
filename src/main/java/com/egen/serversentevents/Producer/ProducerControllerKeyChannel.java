package com.egen.serversentevents.Producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;

/**
 * Producer RouterFunction Controller : send message
 */
@Component
public class ProducerControllerKeyChannel {

    private static final Logger log = LoggerFactory.getLogger(ProducerControllerKeyChannel.class.getName());

    private static final String TOPIC = "egen-sse-test-topic";

    private final KafkaSender<String, String> kafkaSender;   // Mono<Producer>

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");

    @Autowired
    public ProducerControllerKeyChannel(KafkaSender<String, String> kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    @Bean
    public RouterFunction<ServerResponse> getRouterSendFunction() {
        System.out.println("==========router producer===========");

        String message = "";

        return RouterFunctions.route(GET("/kafka-send-messages"), serverRequest -> sendToMessages(dateFormat, kafkaSender, serverRequest, message))
                .andRoute(GET("/kafka-send-messages/{id}"), serverRequest -> sendToMessagesKeyChannel(dateFormat, kafkaSender, serverRequest, message));
    }

    /**
     * message queue 에 data 전송
     * @param dateFormat
     * @param kafkaSender
     * @param serverRequest
     * @param message
     * @return
     */
    private static Mono<ServerResponse> sendToMessages(SimpleDateFormat dateFormat, KafkaSender<String, String> kafkaSender, ServerRequest serverRequest, String message) {

        Optional<String> msg = serverRequest.queryParam("msg");

        // public <T> Flux<SenderResult<T>> send(Publisher<? extends SenderRecord<K, V, T>> records)
        // SenderRecord를 1부터 20까지 생성
        // SenderRecord는 ProducerRecord를 매개변수로 생성
        // 바로 subscribe 하여 reactor를 수행
//        kafkaSender.send((Publisher<? extends SenderRecord<String, String, Integer>>) SenderRecord.create(new ProducerRecord<>(TOPIC, message), 1))
        // SenderRecord extends ProducerRecord(String topic, V value)
        // KafkaTemplate 의 send 메소드 처리 방식과 동일 : producer.send(producerRecord, this.buildCallback(producerRecord, producer, future, sample));
        // send() 는 오퍼레이션(비동기 처리로 Producer 프로세스의 buffer에 저장된 후 전송.) 이유는? send() 호출 시 I/O Block이 발생하여 병렬성이 떨어짐. Bulk로 보내는 것보다 Network Overhead가 발생.
        // 채팅이라는 TOPIC을 하나 만들고 각각의 채팅방은 KEY로 나눠주자...
        kafkaSender.send(Flux.range(1, 1).map(i -> SenderRecord.create(new ProducerRecord<>(TOPIC, msg.get()), i)))
                .doOnError(e -> log.error("Send failed", e))
                .subscribe(r -> {
                    RecordMetadata metadata = r.recordMetadata();   // SenderResult(카프카에서 받은 전송결과 메타데이터) 의 메타 데이터, r.correlationMetadata()

                    System.out.printf("Message %d sent successfully, topic-partition=%s-%d offset=%d timestamp=%s%n", r.correlationMetadata(), metadata.topic(), metadata.partition(), metadata.offset(), dateFormat.format(new Date(metadata.timestamp())));

                });

//        new KafkaAdminClient().deleteTopics()

        return ServerResponse.status(HttpStatus.OK).build();

    }

    /**
     * message queue 에 data 전송
     * @param dateFormat
     * @param kafkaSender
     * @param serverRequest
     * @param message
     * @return
     */
    private static Mono<ServerResponse> sendToMessagesKeyChannel(SimpleDateFormat dateFormat, KafkaSender<String, String> kafkaSender, ServerRequest serverRequest, String message) {

        String key = String.valueOf(serverRequest.pathVariable("id"));

        Optional<String> msg = serverRequest.queryParam("msg");

        // public <T> Flux<SenderResult<T>> send(Publisher<? extends SenderRecord<K, V, T>> records)
        // SenderRecord를 1부터 20까지 생성
        // SenderRecord는 ProducerRecord를 매개변수로 생성
        // 바로 subscribe 하여 reactor를 수행
//        kafkaSender.send((Publisher<? extends SenderRecord<String, String, Integer>>) SenderRecord.create(new ProducerRecord<>(TOPIC, message), 1))
        // SenderRecord extends ProducerRecord(String topic, V value)
        // KafkaTemplate 의 send 메소드 처리 방식과 동일 : producer.send(producerRecord, this.buildCallback(producerRecord, producer, future, sample));
        // send() 는 오퍼레이션(비동기 처리로 Producer 프로세스의 buffer에 저장된 후 전송.) 이유는? send() 호출 시 I/O Block이 발생하여 병렬성이 떨어짐. Bulk로 보내는 것보다 Network Overhead가 발생.
        // 채팅이라는 TOPIC을 하나 만들고 각각의 채팅방은 KEY로 나눠주자...
        kafkaSender.send(Flux.range(1, 1).map(i -> SenderRecord.create(new ProducerRecord<>(TOPIC, key, msg.get()), i)))
                .doOnError(e -> log.error("Send failed", e))
                .subscribe(r -> {
                    RecordMetadata metadata = r.recordMetadata();   // SenderResult(카프카에서 받은 전송결과 메타데이터) 의 메타 데이터, r.correlationMetadata()

                    System.out.printf("Message %d sent successfully, topic-partition=%s-%d offset=%d timestamp=%s%n", r.correlationMetadata(), metadata.topic(), metadata.partition(), metadata.offset(), dateFormat.format(new Date(metadata.timestamp())));

                });

//        new KafkaAdminClient().deleteTopics()

        return ServerResponse.status(HttpStatus.OK).build();

    }

}

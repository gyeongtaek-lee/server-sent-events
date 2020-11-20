package com.egen.serversentevents.Producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
import java.util.Map;
import java.util.Optional;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RequestPredicates.POST;

@Configuration
@Component
public class ProducerController {

    private static final Logger log = LoggerFactory.getLogger(ProducerController.class.getName());

    private static final String TOPIC = "egen-sse-test-topic";

    final
    KafkaSender<String, String> kafkaSender;   // Mono<Producer>

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");

    public ProducerController(KafkaSender<String, String> kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    @Bean
    public RouterFunction<ServerResponse> getRouterSendFunction() {
        System.out.println("==========router producer===========");

        String message = "";

        return RouterFunctions.route(GET("/kafka-send-messages"), serverRequest -> sendToMessages(dateFormat, kafkaSender, serverRequest, message));
    }

    private static Mono<ServerResponse> sendToMessages(SimpleDateFormat dateFormat, KafkaSender<String, String> kafkaSender, ServerRequest serverRequest, String message) {

        Optional<String> msg = serverRequest.queryParam("msg");

        // public <T> Flux<SenderResult<T>> send(Publisher<? extends SenderRecord<K, V, T>> records)
        // SenderRecord를 1부터 20까지 생성
        // SenderRecord는 ProducerRecord를 매개변수로 생성
        // 바로 subscribe 하여 reactor를 수행
//        kafkaSender.send((Publisher<? extends SenderRecord<String, String, Integer>>) SenderRecord.create(new ProducerRecord<>(TOPIC, message), 1))
        kafkaSender.send(Flux.range(1, 1).map(i -> SenderRecord.create(new ProducerRecord<>(TOPIC, msg.get()), i)))
                .doOnError(e -> log.error("Send failed", e))
                .subscribe(r -> {
                    RecordMetadata metadata = r.recordMetadata();   // SenderRecord의 메타 데이터

                    System.out.printf("Message %d sent successfully, topic-partition=%s-%d offset=%d timestamp=%s%n", r.correlationMetadata(), metadata.topic(), metadata.partition(), metadata.offset(), dateFormat.format(new Date(metadata.timestamp())));

                });

        return ServerResponse.status(HttpStatus.OK).build();

    }

}

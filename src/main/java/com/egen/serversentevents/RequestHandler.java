package com.egen.serversentevents;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.config.WebFluxConfigurer;
import org.springframework.web.reactive.function.BodyInserter;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;

import java.util.List;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RequestPredicates.accept;

//    @Configuration
//    @EnableWebFlux
@CrossOrigin(allowedHeaders = "*")
@Configuration
@Component
public class RequestHandler {

    final
    KafkaReceiver<String, String> kafkaReceiver;

    private ConnectableFlux<ServerSentEvent<String>> eventPublisher;

    @Autowired
    public RequestHandler(KafkaReceiver<String, String> kafkaReceiver) {
        // conversion to ConnectableFlux.
        // Alternative to "publish is "replay"
        // which resends all past received Kafka Messages to each new observer
        // replay : 구성 가능한 제한 (시간 및 버퍼 크기)까지 첫 번째 구독을 통해 표시되는 데이터를 버퍼링합니다. 후속 구독자에게 데이터를 재생합니다.
        // publish : 이러한 요청을 소스로 전달하여 역압 측면에서 다양한 가입자의 요구를 동적으로 존중하려고합니다. 특히, 구독자가의 보류중인 수요를 가지고있는 경우 0게시는 소스에 대한 요청을 일시 중지합니다.
        eventPublisher = kafkaReceiver.receive()
                .map(consumerRecord -> {
                        System.out.println("consumer => "+consumerRecord.value());
                    return ServerSentEvent.builder(consumerRecord.value()).build();
                })
                .replay(10);


        // subscribes to the KafkaReceiver -> starts consumption (without observers attached)
        eventPublisher.connect();

        this.kafkaReceiver = kafkaReceiver;
    }

    @CrossOrigin(allowedHeaders = "*")
    @PutMapping("/cors-enabled-endpoint")
    @Bean
    public RouterFunction<ServerResponse> getRouterFunction() {
        System.out.println("==========router consumer===========");
        return RouterFunctions.route(GET("/kafka-messages"), this::subscribeToMessages);
    }

    @CrossOrigin({ "*" })
    private Mono<ServerResponse> subscribeToMessages(ServerRequest serverRequest) {
        System.out.println("==========start===========");
        List<MediaType> list = serverRequest.headers().accept();
        for (int i = 0; i < list.size(); i++) {
            System.out.println("==>"+list.get(i));
        }

        return ServerResponse.status(HttpStatus.OK).body(BodyInserters.fromServerSentEvents(eventPublisher));
    }

}

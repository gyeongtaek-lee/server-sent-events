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
        eventPublisher = kafkaReceiver.receive()
                .map(consumerRecord -> ServerSentEvent.builder(consumerRecord.value()).build())
                .publish();

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
            System.out.println(list.get(i));
        }

        return ServerResponse.status(HttpStatus.OK).body(BodyInserters.fromServerSentEvents(eventPublisher));
    }

}

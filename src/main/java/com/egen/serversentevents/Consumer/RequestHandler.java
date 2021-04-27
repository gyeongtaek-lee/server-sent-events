package com.egen.serversentevents.Consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PutMapping;
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

/**
 * Consumer RouterFunction Controller (handler) : receive message
 */
//    @Configuration
//    @EnableWebFlux
@CrossOrigin(allowedHeaders = "*")
//@Component
public class RequestHandler {

    private final KafkaReceiver<String, String> kafkaReceiver;

    // reactor Flux extends...
    private ConnectableFlux<ServerSentEvent<String>> eventPublisher;

    @Autowired
    public RequestHandler(KafkaReceiver<String, String> kafkaReceiver) {

        this.kafkaReceiver = kafkaReceiver;

        // conversion to ConnectableFlux.
        // Alternative to "publish is "replay"
        // which resends all past received Kafka Messages to each new observer
        // replay : 구성 가능한 제한 (시간 및 버퍼 크기)까지 첫 번째 구독을 통해 표시되는 데이터를 버퍼링합니다. 후속 구독자에게 데이터를 재생합니다.
        // publish : 이러한 요청을 소스로 전달하여 역압 측면에서 다양한 가입자의 요구를 동적으로 존중하려고합니다. 특히, 구독자가의 보류중인 수요를 가지고있는 경우 0게시는 소스에 대한 요청을 일시 중지합니다.
        // receive() : 발송 데이터 가져오는 오퍼레이션 / 카프카에 오프셋을 커밋할 필요가 없는 어플리케이션은, KafkaReceiver#receive()로 컨슘한 모든 레코드에서 acknowledge를 호출하지 않는 식으로 자동 커밋을 비활성화할 수 있다.
        eventPublisher = kafkaReceiver.receive()
                .map(consumerRecord -> {

                    System.out.println("consumer => "+consumerRecord.value());

//                    consumerRecord.receiverOffset().acknowledge();    // 오프셋을 커밋할 수 있게 레코드 처리를 완료했음을 알린다. 커밋 인터벌이나 커밋 배치 사이즈를 설정했다면, 확인된(acknowledged) 오프셋을 주기적으로 커밋한다.

                    return ServerSentEvent.builder(consumerRecord.value()).build();  // ServerSentEvent 리턴

                })
                .replay(10);


        // subscribes to the KafkaReceiver -> starts consumption (without observers attached)
        // Consumer extends Disposable
        // FluxPublish extends ConnectableFlux
        // consumer.accept(s);
        //        if (doConnect) {
        //            this.source.subscribe(s);
        //        }채
        // kafka topic에 kafkaReciver를 이용하여 stream을 연결해 놓고 오퍼레이션을 생성한 후, publisher가 데이터를 발송하면 subcribe(구독) 한다.
        eventPublisher.connect();

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

        // response's body write Mono<Void>
        // ServerSentEventHttpMessageWriter => message.writeAndFlushWith
        // 구독 된 정보를 Mono type으로 server response body에 작성하여 리턴.
        return ServerResponse.status(HttpStatus.OK).body(BodyInserters.fromServerSentEvents(eventPublisher));
    }

}

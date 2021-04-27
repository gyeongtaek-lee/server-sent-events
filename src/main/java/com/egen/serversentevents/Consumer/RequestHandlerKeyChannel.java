package com.egen.serversentevents.Consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
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
@Component
public class RequestHandlerKeyChannel {

    private final KafkaReceiver<String, String> kafkaReceiver;

    // reactor Flux extends...
    private ConnectableFlux<ServerSentEvent<String>> eventPublisher;

    @Autowired
    public RequestHandlerKeyChannel(KafkaReceiver<String, String> kafkaReceiver) {

        this.kafkaReceiver = kafkaReceiver;

        // conversion to ConnectableFlux.
        // Alternative to "publish is "replay"
        // which resends all past received Kafka Messages to each new observer
        // replay : 구성 가능한 제한 (시간 및 버퍼 크기)까지 첫 번째 구독을 통해 표시되는 데이터를 버퍼링합니다. 후속 구독자에게 데이터를 재생합니다.
        // publish : 이러한 요청을 소스로 전달하여 역압 측면에서 다양한 가입자의 요구를 동적으로 존중하려고합니다. 특히, 구독자가의 보류중인 수요를 가지고있는 경우 0게시는 소스에 대한 요청을 일시 중지합니다.
        // receive() : 발송 데이터 가져오는 오퍼레이션
        // Make the Publisher Hot
        eventPublisher = kafkaReceiver.receive()
                .map(consumerRecord -> {

                    System.out.printf("consumer => key : %s , value : %s", consumerRecord.key(), consumerRecord.value());

                    return ServerSentEvent.builder(consumerRecord.value()).id(consumerRecord.key()).build();  // ServerSentEvent 리턴

                })
                .replay(10);  // publish()


        // subscribes to the KafkaReceiver -> starts consumption (without observers attached)
        // Consumer extends Disposable
        // FluxPublish extends ConnectableFlux
        // consumer.accept(s);
        //        if (doConnect) {
        //            this.source.subscribe(s);
        //        }채
        // kafka topic에 kafkaReciver를 이용하여 stream을 연결해 놓고 오퍼레이션을 생성한 후, publisher가 데이터를 발송하면 subcribe(구독) 한다.
        // , 이 메서드가 여러 구독자들이 Connectable Flux를 구독한 후 값을 생성하여 각 구독자에게 보내기 시작하게 하는 메서드이다.
        eventPublisher.connect();

        /**
         * ex)
         * // Now that we have a handle on the hot Publisher
         * // Let's subscribe to that with multiple subscribers. (first subscriber)
         * connectableFlux.subscribe(i -> System.out.println("first_subscriber received value:" + i));
         * // Start firing events with .connect() on the published flux.
         * connectableFlux.connect();
         * Thread.sleep(3_000);
         * // Let a second subscriber come after some time 3 secs here. (second subscriber)
         * connectableFlux.subscribe(i -> System.out.println("second_subscriber received value:" + i));
         * result) hot publisher일 경우 : 결과에서 두 번째 구독자 는 3 초부터 값을 얻고 0,1,2를 놓친다 는 것을 알 수 있습니다 .
         * first_subscriber received value:0
         * first_subscriber received value:1
         * first_subscriber received value:2
         * first_subscriber received value:3
         * second_subscriber received value:3
         * first_subscriber received value:4
         * second_subscriber received value:4
         */


    }

    @CrossOrigin(allowedHeaders = "*")
    @GetMapping("/cors-enabled-endpoint")
    @Bean
    public RouterFunction<ServerResponse> getRouterFunction() {
        System.out.println("==========router consumer===========");
        return RouterFunctions.route(GET("/kafka-messages"), this::subscribeToMessages)
                .andRoute(GET("/kafka-messages/{id}"), this::subscribeToMessagesKeyChannel);
    }

    @CrossOrigin({ "*" })
    private Mono<ServerResponse> subscribeToMessages(ServerRequest serverRequest) {
        System.out.println("==========start default===========");

        List<MediaType> list = serverRequest.headers().accept();
        for (int i = 0; i < list.size(); i++) {
            System.out.println("==>"+list.get(i));
        }

        // response's body write Mono<Void>
        // ServerSentEventHttpMessageWriter => message.writeAndFlushWith
        // 구독 된 정보를 Mono type으로 server response body에 작성하여 리턴.
        return ServerResponse.status(HttpStatus.OK).body(BodyInserters.fromServerSentEvents(eventPublisher));
    }

    @CrossOrigin({ "*" })
    private Mono<ServerResponse> subscribeToMessagesKeyChannel(ServerRequest serverRequest) {
        System.out.println("==========start key channel===========");

        String id = String.valueOf(serverRequest.pathVariable("id"));

        // response's body write Mono<Void>
        // ServerSentEventHttpMessageWriter => message.writeAndFlushWith
        // 구독 된 정보를 Mono type으로 server response body에 작성하여 리턴.
        return ServerResponse.status(HttpStatus.OK).body(BodyInserters.fromServerSentEvents(keyChannel(id)));
    }

    /**
     * 사용자 id별 분기 처리
     * @param id
     * @return
     */
    private ConnectableFlux<ServerSentEvent<String>> keyChannel(String id) {

        ConnectableFlux<ServerSentEvent<String>> connectableFlux = eventPublisher.filter(sse -> id.equals(sse.id())).publish();

        connectableFlux.connect();

        return connectableFlux;
    }

}

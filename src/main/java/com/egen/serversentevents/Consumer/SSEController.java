package com.egen.serversentevents.Consumer;

import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

/**
 * REST Controller Class
 * @RestController는 @Controller + @ResponseBody가 합쳐진 것 : 컨트롤러의 모든 HTTP 요청 처리 메서드에서 HTTP 응답 몸채에 직접 쓰는 값을 반환한다는 것을 스프링에게 알려줌.
 * 즉 @Controller 는 @ResponseBody를 메서드에 지정해야 하지만 @RestController는 필요 없다.
 * 반환을 View가 아닌 Json 형태로 객체 데이터를 Client에게 반환한다.
 *
 * localhost:8080/sse 로 연결한 client에게 kafka의 Producer가 record를 보내면
 * 이것을 SpringSSEConfigurations 에 선언한 kafkaReceiver(Consumer)가 해당 토픽의 record를 받아 (Flux<ReceiverRecord<String, String>> kafkaFlux = kafkaReceiver.receive();)
 * 이 값을 client에게 반환한다.
 *
 * 이를 chatting으로 구현하면 Producer는 채팅을 입력하는 client의 데이터를 받으면 되고
 * Consumer는 해당 Controller의 토픽에 연결된 모든 채팅 참여자의 client에게 반환하면 된다.
 */
@RestController
@RequestMapping(path = "/sse")
public class SSEController {

    private final KafkaReceiver<String, String> kafkaReceiver;

    private ConnectableFlux<ServerSentEvent<String>> eventPublisher;

    public SSEController(KafkaReceiver<String, String> kafkaReceiver) {

        this.kafkaReceiver = kafkaReceiver;

    }

    /**
     * MediaType.TEXT_EVENT_STREAM_VALUE를 컨텐츠 유형으로 사용.
     * 이는 클라이언트에게 연결이 설정되고 서버에서 클라이언트로 이벤트를 보내기 위해 스트림이 열려 있음을 알려준다.
     * produces는 HTTP 응답 헤더로 쓰이는 컨텐츠 유형을 선언.
     * @return
     *
     * test
     * curl.exe -L -X GET 'localhost:8080/sse' -H 'Content-Type: text/event-stream; charset=UTF-8' -H 'Accept: text/event-stream; charset=UTF-8'
     */
    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Flux getEventsFlux(){
//    Mono<ServerResponse> getEventsFlux(){

//        eventPublisher = kafkaReceiver.receive().map(consumerRecord -> ServerSentEvent.builder(consumerRecord.value()).build()).publish();

        // subscribes to the KafkaReceiver -> starts consumption (without observers attached)
//        eventPublisher.connect();

        // reactive flux 형으로 message key, value instance
        Flux<ReceiverRecord<String, String>> kafkaFlux = kafkaReceiver.receive();

        // ReceiverOffset 은 해당 레코드(ReceiverRecord)가 처리 된 후 확인되어야하는 토픽의 파티션에 대한 오프셋 인스턴스이다.
        // acknowledge(승인) : 이 ReceiverRecord가 오프셋과 관련된지를 확인. 오프셋은 커밋 구성 매개 변수인 ReceiverOptions의 commitInterval()과 commitBatchSize() 승인
        // 오프셋이 승인이 되면 이 오프셋을 포함하여 해당 파티션의 모든 레코드가 처리 된 것으로 간주한다.
        // 이후 수신자가 Flux를 종료하는 시점에 가능한 경우 모든 승인이 된 offset을 커밋한다. 즉, acknowledge는 커밋이 될 수 있는 상태로 승인을 해주는 행위이다.
        // map의 loop를 통해 ReceiverRecord return...
        // checkpoint : 시퀀스(Flux)가 신호를 발생하는 과정에서 익셉션이 발생하면 어떻게 될까? 시퀀스가 여러 단게를 거쳐 변환한다면 어떤 시점에 익셉션이 발생했는지 단번에 찾기 힘들 수도 있다. 이럴 때 도움이 되는 것이 체크포인트이다.
        return kafkaFlux.checkpoint("Messages are started begin consumed").log().doOnNext(r -> r.receiverOffset().acknowledge())
                .map(r -> r.key() + " : " + r.value()).checkpoint("Messages are done consumed");
//        return ServerResponse.status(HttpStatus.OK).body(BodyInserters.fromServerSentEvents(eventPublisher));

    }

}

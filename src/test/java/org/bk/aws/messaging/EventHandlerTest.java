package org.bk.aws.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class EventHandlerTest {

    private SqsEventHandler<JsonNode> eventHandler;

    @Mock
    private SqsAsyncClient sqsAsyncClient;

    @Mock
    private SnsAsyncClient snsAsyncClient;


    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @BeforeEach
    void setUp() {
        eventHandler = new SqsEventHandler<>(snsAsyncClient, sqsAsyncClient,
                OBJECT_MAPPER, "queue", "topicName");
    }

    @Test
    void testProcessingPipelineCleanFlow() throws Exception {
        SqsEventHandler.SnsNotification msg1 = new SqsEventHandler.SnsNotification("{}");

        SqsEventHandler.SnsNotification msg2 = new SqsEventHandler.SnsNotification("{}");

        when(sqsAsyncClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(completedFuture(ReceiveMessageResponse
                        .builder()
                        .messages(Message
                                        .builder()
                                        .body(OBJECT_MAPPER.writeValueAsString(msg1))
                                        .build(),
                                Message
                                        .builder()
                                        .body(OBJECT_MAPPER.writeValueAsString(msg2))
                                        .build())
                        .build()))
                .thenReturn(completedFuture(ReceiveMessageResponse
                        .builder()
                        .build()));

        Flux<MessageWithDeleteHandle<JsonNode>> flux = eventHandler
                .listen(1, JsonNode.class)
                .subscribeOn(Schedulers.newSingle("test"));

        ObjectNode expected = OBJECT_MAPPER.createObjectNode();
        StepVerifier.create(flux)
                .assertNext((MessageWithDeleteHandle<JsonNode> messageAndHandle) -> {
                    ContentMessage<JsonNode> contentMessage = messageAndHandle.getMessage();
                    assertThat(contentMessage.getBody()).isEqualTo(expected);
                })
                .assertNext((MessageWithDeleteHandle<JsonNode> messageAndHandle) -> {
                    ContentMessage<JsonNode> contentMessage = messageAndHandle.getMessage();
                    assertThat(contentMessage.getBody()).isEqualTo(expected);
                })
                .then(() -> eventHandler.setRunning(false))
                .verifyComplete();
    }

    @Test
    void testProcessingPipelineWithErrors() throws Exception {
        when(sqsAsyncClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(completedFuture(ReceiveMessageResponse
                        .builder()
                        .messages(Message
                                        .builder()
                                        .body(OBJECT_MAPPER.writeValueAsString(new SqsEventHandler.SnsNotification("{}")))
                                        .build(),
                                Message
                                        .builder()
                                        .body(OBJECT_MAPPER.writeValueAsString(new SqsEventHandler.SnsNotification("{}")))
                                        .build())
                        .build()))
                .thenReturn(failedFuture(new RuntimeException("failed retrieving messages")))
                .thenReturn(completedFuture(ReceiveMessageResponse
                        .builder()
                        .messages(Message
                                        .builder()
                                        .body(OBJECT_MAPPER.writeValueAsString(new SqsEventHandler.SnsNotification("{}")))
                                        .build(),
                                Message
                                        .builder()
                                        .body(OBJECT_MAPPER.writeValueAsString(new SqsEventHandler.SnsNotification("{}")))
                                        .build())
                        .build()))
                .thenReturn(completedFuture(ReceiveMessageResponse
                        .builder()
                        .build()));

        Flux<MessageWithDeleteHandle<JsonNode>> flux = eventHandler
                .listen(1, JsonNode.class)
                .subscribeOn(Schedulers.newSingle("test"));

        ObjectNode expected = OBJECT_MAPPER.createObjectNode();
        StepVerifier.create(flux)
                .assertNext((MessageWithDeleteHandle<JsonNode> messageAndHandle) -> {
                    ContentMessage<JsonNode> contentMessage = messageAndHandle.getMessage();
                    assertThat(contentMessage.getBody()).isEqualTo(expected);
                })
                .assertNext((MessageWithDeleteHandle<JsonNode> messageAndHandle) -> {
                    ContentMessage<JsonNode> contentMessage = messageAndHandle.getMessage();
                    assertThat(contentMessage.getBody()).isEqualTo(expected);
                })
                .assertNext((MessageWithDeleteHandle<JsonNode> messageAndHandle) -> {
                    ContentMessage<JsonNode> contentMessage = messageAndHandle.getMessage();
                    assertThat(contentMessage.getBody()).isEqualTo(expected);
                })
                .assertNext((MessageWithDeleteHandle<JsonNode> messageAndHandle) -> {
                    ContentMessage<JsonNode> contentMessage = messageAndHandle.getMessage();
                    assertThat(contentMessage.getBody()).isEqualTo(expected);
                })
                .then(() -> eventHandler.setRunning(false))
                .verifyComplete();
    }

}

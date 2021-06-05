package org.bk.aws.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bk.aws.junit5.SnsAndSqsTestExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.net.URI;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class SqsEventHandlerIntegrationTest {
    @RegisterExtension
    public static SnsAndSqsTestExtension snsSqsContainer = new SnsAndSqsTestExtension();
    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @Tag("integration")
    void testQueueStream() {
        SnsAsyncClient snsAsyncClient = snsAsyncClient();
        SqsAsyncClient sqsAsyncClient = sqsAsyncClient();
        SqsEventHandler<JsonNode> snsSqsEventHandler = new SqsEventHandler<>(snsAsyncClient, sqsAsyncClient,
                objectMapper, "sampleQueue", "sampleTopic");
        SnsMessageSender snsMessageSender = new SnsMessageSender(snsAsyncClient, objectMapper, "sampleTopic");

        ObjectNode m1 = objectMapper.createObjectNode().put("key", "someValue1");
        ObjectNode m2 = objectMapper.createObjectNode().put("key", "someValue2");
        snsSqsEventHandler.init();
        snsMessageSender.init();

        snsMessageSender.send(m1)
                .then(snsMessageSender.send(m2))
                .subscribe();

        StepVerifier
                .create(snsSqsEventHandler
                        .processWithResultStream(2, "testTask",
                                jsonNode -> Mono.just(jsonNode), JsonNode.class)
                )
                .assertNext(contentMessage -> assertThat(contentMessage.getBody()).isEqualTo(m1))
                .assertNext(contentMessage -> assertThat(contentMessage.getBody()).isEqualTo(m2))
                .then(() -> snsSqsEventHandler.setRunning(false))
                .verifyComplete();
    }

    @BeforeAll
    static void beforeAll() {
        StepVerifier.setDefaultTimeout(Duration.ofSeconds(100));
    }

    @AfterAll
    static void tearDown() {
        StepVerifier.resetDefaultTimeout();
    }


    public SnsAsyncClient snsAsyncClient() {
        return SnsAsyncClient
                .builder()
                .endpointOverride(URI.create(snsSqsContainer.getSnsEndpoint()))
                .build();
    }

    public SqsAsyncClient sqsAsyncClient() {
        return SqsAsyncClient
                .builder()
                .endpointOverride(URI.create(snsSqsContainer.getSqsEndpoint()))
                .build();
    }
}

package org.bk.aws.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import javax.annotation.PostConstruct;


/**
 * Responsible for sending messages to SNS
 */
public class SnsMessageSender {
    private final SnsAsyncClient snsAsyncClient;
    private final ObjectMapper objectMapper;

    private final String topicName;
    private String topicArn;

    public SnsMessageSender(SnsAsyncClient snsAsyncClient, ObjectMapper objectMapper, String topicName) {
        this.snsAsyncClient = snsAsyncClient;
        this.objectMapper = objectMapper;
        this.topicName = topicName;
    }

    @PostConstruct
    public void init() {
        //Potential of a race condition between SqsEventHandler
        // and the logic here. Control order using the `@DependsOn` annotation
        QueueProvisioningUtils.createTopic(snsAsyncClient, topicName)
                .doOnNext(topicDetails -> {
                    // safe to do as this is a blocking call at the end of it
                    this.topicArn = topicDetails.getTopicArn();
                })
                .block();
    }

    /**
     * Send a message to SNS
     *
     * @param message string format of the message to send to sns
     * @return a Mono that responds to completion signals and does not emit individual messages
     */
    public Mono<Void> send(String message) {
        return Mono.defer(() -> {
            final PublishRequest publishRequest =
                    PublishRequest.builder().topicArn(this.topicArn).message(message).build();
            return Mono.fromFuture(snsAsyncClient.publish(publishRequest));
        }).then();
    }

    /**
     * Send a message to SNS
     *
     * @param message to send to sns. Message will be serialized to json before being sent
     * @return a Mono that responds to completion signals and does not emit individual messages
     * @param <T> type of message to be sent
     */
    public <T> Mono<Void> send(T message) {
        String messageAsString = getMessageAsString(message);
        return send(messageAsString);
    }

    private <T> String getMessageAsString(T message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException jsonProcessingException) {
            throw new RuntimeException(jsonProcessingException);
        }
    }
}

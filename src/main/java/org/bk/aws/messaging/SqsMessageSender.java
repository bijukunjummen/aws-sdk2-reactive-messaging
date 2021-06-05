package org.bk.aws.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import javax.annotation.PostConstruct;

/**
 * Responsible for sending messages to SQS
 */
public class SqsMessageSender {
    private final SqsAsyncClient sqsAsyncClient;
    private final ObjectMapper objectMapper;

    private final String queueName;
    private final String dlqName;
    private String queueUrl;

    public SqsMessageSender(SqsAsyncClient sqsAsyncClient, ObjectMapper objectMapper, String queueName, String dlqName) {
        this.sqsAsyncClient = sqsAsyncClient;
        this.objectMapper = objectMapper;
        this.queueName = queueName;
        this.dlqName = dlqName;
    }

    @PostConstruct
    public void init() {
        //Potential of a race condition between SqsEventHandler
        // and the logic here. Control order using the `@DependsOn` annotation
        QueueProvisioningUtils.createPrimaryAndDeadLetterQueues(sqsAsyncClient, queueName, dlqName)
                .doOnNext(queueDetails -> {
                    // safe to do as this is a blocking call at the end of it
                    this.queueUrl = queueDetails.getQueueUrl();
                })
                .block();
    }

    /**
     * Send a message to SNS
     *
     * @param message
     * @return a Mono that responds to completion signals and does not emit individual messages
     */
    public Mono<Void> send(String message) {
        return Mono.defer(() -> {
            final SendMessageRequest publishRequest =
                    SendMessageRequest.builder().queueUrl(this.queueUrl).messageBody(message).build();
            return Mono.fromFuture(sqsAsyncClient.sendMessage(publishRequest));
        }).then();
    }

    /**
     * Send a message to SNS
     *
     * @param message
     * @param builder custom message builder
     * @return a Mono that responds to completion signals and does not emit individual messages
     */
    public Mono<Void> send(String message, SendMessageRequest.Builder builder) {
        return Mono
                .defer(() -> {
                    final SendMessageRequest publishRequest = builder.queueUrl(this.queueUrl).messageBody(message).build();
                    return Mono.fromFuture(sqsAsyncClient.sendMessage(publishRequest));
                })
                .then();
    }

    /**
     * Send a message to SNS
     *
     * @param message
     * @return a Mono that responds to completion signals and does not emit individual messages
     */
    public <T> Mono<Void> send(T message) {
        String messageAsString = getMessageAsString(message);
        return send(messageAsString);
    }

    /**
     * Send a message to SNS
     *
     * @param message
     * @param builder custom message builder
     * @return a Mono that responds to completion signals and does not emit individual messages
     */
    public <T> Mono<Void> send(T message, SendMessageRequest.Builder builder) {
        String messageAsString = getMessageAsString(message);
        return send(messageAsString, builder);
    }

    private <T> String getMessageAsString(T message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException jsonProcessingException) {
            throw new RuntimeException(jsonProcessingException);
        }
    }
}

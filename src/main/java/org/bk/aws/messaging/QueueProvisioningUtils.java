package org.bk.aws.messaging;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueNameExistsException;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesResponse;

import java.util.Map;

import static reactor.function.TupleUtils.function;

public final class QueueProvisioningUtils {
    private QueueProvisioningUtils() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueProvisioningUtils.class);

    public static final int DEFAULT_TIMEOUT = 60;
    public static final int DEFAULT_MAX_RECEIVE_COUNT = 5;

    public static class QueueDetails {
        private final String queueUrl;
        private final String queueArn;

        public QueueDetails(String queueUrl, String queueArn) {
            this.queueUrl = queueUrl;
            this.queueArn = queueArn;
        }

        public String getQueueUrl() {
            return queueUrl;
        }

        public String getQueueArn() {
            return queueArn;
        }
    }

    public static class TopicDetails {
        private final String topicName;
        private final String topicArn;

        public TopicDetails(String topicName, String topicArn) {
            this.topicName = topicName;
            this.topicArn = topicArn;
        }

        public String getTopicName() {
            return topicName;
        }

        public String getTopicArn() {
            return topicArn;
        }
    }

    public static Mono<QueueDetails> createPrimaryAndDeadLetterQueues(SqsAsyncClient sqsAsyncClient, String mainQueueName, String dlqName) {
        // First create the DLQ
        return createDeadLetterQueue(sqsAsyncClient, dlqName)
                // Create primary queue
                .flatMap(dlqDetails -> Mono.zip(createPrimaryQueue(sqsAsyncClient, mainQueueName, dlqDetails.getQueueArn()),
                        Mono.just(dlqDetails)))
                // Loop them together
                .flatMap(function((main, dlq) -> Mono.zip(Mono.just(main), updateDeadLetterRedrive(sqsAsyncClient, main, dlq))))
                // Return the main queue details
                .map(function((main, dlq) -> main));
    }


    public static Mono<QueueDetails> createDeadLetterQueue(SqsAsyncClient sqsAsyncClient, String dlqName) {
        final CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(dlqName)
                .build();

        return createQueue(sqsAsyncClient, createQueueRequest);
    }

    public static Mono<QueueDetails> createPrimaryQueue(SqsAsyncClient sqsAsyncClient, String queueName, String dlqArn) {
        final ObjectNode redrivePolicy = JsonNodeFactory.instance.objectNode()
                .put("maxReceiveCount", DEFAULT_MAX_RECEIVE_COUNT)
                .put("deadLetterTargetArn", dlqArn);

        final CreateQueueRequest queueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributes(Map.of(
                        QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(DEFAULT_TIMEOUT),
                        QueueAttributeName.REDRIVE_POLICY, redrivePolicy.toString()))
                .build();

        return createQueue(sqsAsyncClient, queueRequest);
    }

    public static Mono<SetQueueAttributesResponse> updateDeadLetterRedrive(SqsAsyncClient sqsAsyncClient, QueueDetails primary, QueueDetails deadLetter) {
        final ObjectNode redrivePolicy = JsonNodeFactory.instance.objectNode()
                .put("maxReceiveCount", 1)
                .put("deadLetterTargetArn", primary.getQueueArn());

        final SetQueueAttributesRequest setQueueAttributesRequest = SetQueueAttributesRequest.builder()
                .queueUrl(deadLetter.getQueueUrl())
                .attributes(Map.of(QueueAttributeName.REDRIVE_POLICY, redrivePolicy.toString()))
                .build();

        return Mono.fromFuture(sqsAsyncClient.setQueueAttributes(setQueueAttributesRequest));
    }

    /**
     * Generic queue creation method.
     *
     * @param  sqsAsyncClient sqs client
     * @param createQueueRequest Queue Request
     * @return Mono containing queue details (URL and ARN)
     */
    public static Mono<QueueDetails> createQueue(SqsAsyncClient sqsAsyncClient, CreateQueueRequest createQueueRequest) {
        return Mono.fromFuture(sqsAsyncClient.createQueue(createQueueRequest))
                .map(CreateQueueResponse::queueUrl)
                // Handle QueueNameExistsException by updating attributes and returning the Queue URL from name
                .onErrorResume(QueueNameExistsException.class, e -> {
                    LOGGER.info("Queue name={} already exists, updating attributes...", createQueueRequest.queueName());

                    final GetQueueUrlRequest queueUrlRequest = GetQueueUrlRequest.builder()
                            .queueName(createQueueRequest.queueName())
                            .build();

                    return Mono.fromFuture(sqsAsyncClient.getQueueUrl(queueUrlRequest))
                            .flatMap(response -> {
                                final String queueUrl = response.queueUrl();
                                final SetQueueAttributesRequest queueAttributesRequest = SetQueueAttributesRequest.builder()
                                        .queueUrl(queueUrl)
                                        .attributes(createQueueRequest.attributes())
                                        .build();

                                return Mono.fromFuture(sqsAsyncClient.setQueueAttributes(queueAttributesRequest))
                                        .thenReturn(queueUrl);
                            });
                })
                // Fetch the queueArn now that we know the URL
                .flatMap(url -> Mono.zip(Mono.just(url), getQueueArn(sqsAsyncClient, url)))
                .map(function(QueueDetails::new));
    }

    public static Mono<TopicDetails> createTopic(SnsAsyncClient snsAsyncClient, String topicName) {
        // Get or create topic
        final CreateTopicRequest createTopicRequest = CreateTopicRequest.builder()
                .name(topicName)
                .build();

        return Mono.fromFuture(snsAsyncClient.createTopic(createTopicRequest))
                .map(CreateTopicResponse::topicArn)
                .map(topicArn -> new TopicDetails(topicName, topicArn));
    }

    /**
     * Gets the queue ARN from the queue URL. This assumes the queue has already been created otherwise it will throw
     * an AWS SDK exception.
     *
     * @param sqsAsyncClient SQS Async Client
     * @param queueUrl       Queue URL
     * @return Mono containing Queue ARN
     */
    public static Mono<String> getQueueArn(SqsAsyncClient sqsAsyncClient, String queueUrl) {
        final GetQueueAttributesRequest getQueueAttributeRequest = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.QUEUE_ARN)
                .build();

        return Mono.fromFuture(sqsAsyncClient.getQueueAttributes(getQueueAttributeRequest))
                .map(response -> response.attributes().get(QueueAttributeName.QUEUE_ARN));
    }

    /**
     * Wrapper to simplify deleting messages from SQS Async Client.
     *
     * @param sqsAsyncClient SQS Async Client
     * @param queueUrl       Queue URL
     * @param message        Message to delete
     * @return Mono containing the response
     */
    public static Mono<DeleteMessageResponse> deleteMessage(SqsAsyncClient sqsAsyncClient, String queueUrl, Message message) {
        final DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();

        return Mono.fromFuture(sqsAsyncClient.deleteMessage(deleteMessageRequest));
    }

    /**
     * Bridges an SNS topic to an SQS queue using the Async clients, the topic ARN, and the details of the queue.
     *
     * @param sqsAsyncClient SQS Async Client
     * @param snsAsyncClient SNS Async Client
     * @param topicArn       SNS Topic ARN to bridge
     * @param queueDetails   QueueDetails for queue to process
     * @return Mono that completes when successful
     */
    public static Mono<SetQueueAttributesResponse> bridgeTopicToSqsQueue(SqsAsyncClient sqsAsyncClient,
                                                                         SnsAsyncClient snsAsyncClient,
                                                                         String topicArn,
                                                                         QueueDetails queueDetails) {
        final SubscribeRequest subscribeRequest = SubscribeRequest.builder()
                .protocol("sqs")
                .endpoint(queueDetails.getQueueArn())
                .returnSubscriptionArn(true)
                .topicArn(topicArn)
                .build();

        // Send subscribe request
        return Mono.fromFuture(snsAsyncClient.subscribe(subscribeRequest))
                .doOnSuccess(ignored -> LOGGER.info("Subscribed queue ARN={} to topic ARN={}, subscription={}",
                        queueDetails.getQueueArn(), topicArn, subscribeRequest))
                .then(Mono.defer(() -> {
                    // Once subscribed, the SQS queue needs permissions to receive messages from the SNS topic, so
                    // set that up

                    final SetQueueAttributesRequest setRequest = SetQueueAttributesRequest.builder()
                            .queueUrl(queueDetails.queueUrl)
                            .build();

                    return Mono.fromFuture(sqsAsyncClient.setQueueAttributes(setRequest));
                }))
                .doOnSuccess(response -> LOGGER.info("Policy for queue={} updated to receive messages from SNS",
                        queueDetails.getQueueUrl()));
    }
}

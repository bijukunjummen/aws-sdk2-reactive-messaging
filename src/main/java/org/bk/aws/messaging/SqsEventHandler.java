package org.bk.aws.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bk.aws.model.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class SqsEventHandler<T> implements EventHandler<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsEventHandler.class);
    public static final int WAIT_TIME_SECONDS = 20;
    private static final String DEAD_QUEUE_SUFFIX = "-dead";

    private final SnsAsyncClient snsAsyncClient;
    private final SqsAsyncClient sqsAsyncClient;
    private final ObjectMapper objectMapper;
    private final String queueName;
    private final String snsTopicName;
    private final boolean unwrapSnsMessage;

    // To hold information about queue and topic typically only available during creation flow.
    private String queueUrl;

    private final AtomicBoolean running = new AtomicBoolean(true);

    // For cases where the listener is pure sqs processor
    public SqsEventHandler(SqsAsyncClient sqsAsyncClient, ObjectMapper objectMapper, String queueName) {
        this(null, sqsAsyncClient, objectMapper, queueName, null, false);
    }


    public SqsEventHandler(SnsAsyncClient snsAsyncClient, SqsAsyncClient sqsAsyncClient,
                           ObjectMapper objectMapper, String queueName,
                           String snsTopicName) {
        this(snsAsyncClient, sqsAsyncClient, objectMapper, queueName, snsTopicName, (snsTopicName != null));
    }

    public SqsEventHandler(SnsAsyncClient snsAsyncClient, SqsAsyncClient sqsAsyncClient,
                           ObjectMapper objectMapper, String queueName,
                           String snsTopicName, boolean unwrapSnsMessage) {
        this.snsAsyncClient = snsAsyncClient;
        this.sqsAsyncClient = sqsAsyncClient;
        this.objectMapper = objectMapper;
        this.queueName = queueName;
        this.snsTopicName = snsTopicName;

        // For cases the listening topic may not be under callers control, so the topic name may not be set but
        // still the caller may desire the SNS message unwrapping
        // or for cases where raw sns delivery is enabled
        this.unwrapSnsMessage = unwrapSnsMessage;
    }

    @Override
    public Flux<MessageWithDeleteHandle<T>> listen(int concurrency, Class<T> clazz) {
        return Flux
                .<List<Message>>generate((sink) -> {
                    if (running.get()) {
                        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                                .queueUrl(queueUrl)
                                .maxNumberOfMessages(concurrency)
                                .waitTimeSeconds(WAIT_TIME_SECONDS)
                                .build();

                        try {
                            // SynchronousSink requires the messages to be dispatched in the calling thread
                            // that is the reason to use a blocking call
                            List<Message> messages = sqsAsyncClient
                                    .receiveMessage(receiveMessageRequest)
                                    .get()
                                    .messages();
                            LOGGER.info("Emitting : {}", messages);
                            sink.next(messages);
                        } catch (Exception e) {
                            LOGGER.error("Error in retrieving messages", e);
                            sink.error(e);
                        }
                    } else {
                        sink.complete();
                    }
                })
                .flatMapIterable(Function.identity())
                .doOnError((Throwable t) -> {
                    LOGGER.error(t.getMessage(), t);
                })
                .retry() // Ensure that flux never breaks out
                .map((Message sqsMessage) -> Result.of(() -> {
                    String body = sqsMessage.body();
                    T message = unwrapIfNeeded(body, clazz, unwrapSnsMessage);
                    return new MessageWithDeleteHandle<>(new ContentMessage<>(message), deleteQueueMessage(sqsMessage, queueUrl));
                }))
                // if something goes wrong in unmarshalling of message, then log and filter out the message
                .filter(result -> {
                    if (result.isFailure()) {
                        LOGGER.error("Processing failed on unmarshalling message", result.getCause());
                    }
                    return result.isSuccess();
                })
                .map(result -> result.get());
    }


    @Override
    public <V> Flux<ContentMessage<V>> processWithResultStream(int concurrency, String taskName, Function<T, Mono<V>> task, Class<T> clazz) {
        final Scheduler taskScheduler = Schedulers.newElastic(taskName);
        // It is important to call flatMap with an explicit concurrency parameter
        // Otherwise flatMap tends to buffer a large amount from upstream (256 typically)
        return listen(concurrency, clazz)
                // Just to disconnect from the calling thread
                .subscribeOn(Schedulers.newSingle(taskName))
                .flatMap(sqsMessageAndDeleteHandle -> Mono.defer(() -> {
                    ContentMessage<T> message = sqsMessageAndDeleteHandle.getMessage();
                    Mono<Void> deleteHandle = sqsMessageAndDeleteHandle.getDeleteHandle();
                    LOGGER.debug("Processing: {}", message);
                    return task.apply(message.getBody())
                            .map(message::withNewBody)
                            .defaultIfEmpty(message.withNewBody(null))
                            .flatMap(result -> deleteHandle.thenReturn(result))
                            // Protecting the pipeline against an exception when processing the task
                            .onErrorResume((Throwable t) -> {
                                //message would not have been deleted had the task thrown any runtime exceptoins
                                LOGGER.error("Error in processing task", t);
                                return Mono.empty();
                            })
                            // Do the inner subscription on a scheduler to parallelize task
                            // specified inside the flatMap operation
                            .subscribeOn(taskScheduler)
                            // Otherwise the publish ends up happening on the thread internally used by the CompletableFuture in deleteHandle
                            .publishOn(taskScheduler);
                }), concurrency);
    }

    @Override
    public <V> void processMessage(int concurrency, String taskName, Function<T, Mono<V>> task, Class<T> clazz) {
        processWithResultStream(concurrency, taskName, task, clazz)
                .subscribe(res -> LOGGER.info("Completed Processing {}", res),
                        // If this happens then the pipeline has entirely failed and likely that instance will have to be
                        // recycled. It will be important to find out how an instance ends up in such a state.
                        t -> LOGGER.error("Processing Pipeline failed..", t));
    }

    @PostConstruct
    public void init() {
        QueueProvisioningUtils.createPrimaryAndDeadLetterQueues(sqsAsyncClient, queueName, queueName + DEAD_QUEUE_SUFFIX)
                .doOnNext(queueDetails -> {
                    // safe to do as this is a blocking call at the end of it
                    this.queueUrl = queueDetails.getQueueUrl();
                })
                .flatMap(queueDetails -> {
                    // Create topic if topic name is set, else assume no bridging is required or that bridging
                    // has been done externally
                    if (snsTopicName != null) {
                        return QueueProvisioningUtils.createTopic(snsAsyncClient, snsTopicName)
                                .map(topicDetails ->
                                        QueueProvisioningUtils.bridgeTopicToSqsQueue(sqsAsyncClient, snsAsyncClient,
                                                topicDetails.getTopicArn(), queueDetails));

                    } else {
                        return Mono.empty();
                    }
                })
                //Wait for creation of queues, topics, subscription to finish
                .block();
    }

    @PreDestroy
    public void preDestroy() {
        setRunning(false);
    }

    public void setRunning(boolean running) {
        this.running.set(running);
    }

    private Mono<Void> deleteQueueMessage(Message message, String queueUrl) {
        return Mono
                .defer(() -> QueueProvisioningUtils.deleteMessage(sqsAsyncClient, queueUrl, message))
                .onErrorResume(t -> {
                    LOGGER.error("Error when deleting message from queue: {}, message: {}",
                            queueUrl, message, t);
                    return Mono.empty();
                })
                .then();
    }

    private T unwrapIfNeeded(String body, Class<T> clazz, boolean unwrapSnsMessage) {
        try {
            String toUnMarshal = body;
            if (unwrapSnsMessage) {
                SnsNotification snsNotification = objectMapper.readValue(body, SnsNotification.class);
                toUnMarshal = snsNotification.getMessage();
            }
            return objectMapper.readValue(toUnMarshal, clazz);
        } catch (JsonProcessingException jsonProcessingException) {
            throw new RuntimeException(jsonProcessingException);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class SnsNotification {
        private final String message;

        @JsonCreator
        SnsNotification(@JsonProperty("Message") String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }
}

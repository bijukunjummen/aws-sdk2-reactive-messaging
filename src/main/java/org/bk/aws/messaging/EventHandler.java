package org.bk.aws.messaging;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public interface EventHandler<T> {

    /**
     * Listens to a queue and generates a stream of messages along with a function handle to delete the message
     * once processing is complete
     * <p>
     * This API is recommended only if the caller wants ultimate control over the stream of data from SQS and understands
     * the nuances of how concurrency can be controlled and when to delete the message from the queue.
     * <p>
     * For most uses, the recommendation is to just use the @{link processMessage} call which handles all potential scenarios
     *
     * @param concurrency fixes up the concurrency in processing the messages
     * @param clazz       type of message
     * @return a flux of message
     */
    Flux<MessageWithDeleteHandle<T>> listen(int concurrency, Class<T> clazz);

    /**
     * Listens to a queue, processes the message using a provided task handler,
     * generates a stream of output from the task
     *
     * @param concurrency fixes up the concurrency in processing the messages
     * @param taskName    A descriptive name of the task that the handler will execute, to be used in log messages
     * @param task        - task represented as a Mono. Takes in the message as a parameter and take some action on
     *                    the message
     * @param clazz       - Type of message
     * @return pipeline which returns the output from the task
     */
    <V> Flux<ContentMessage<V>> processWithResultStream(int concurrency, String taskName, Function<T, Mono<V>> task, Class<T> clazz);

    /**
     * Creates end to end pipeline to process a message incuding the final subscription.
     * Ensure that this is called EXACTLY once per task type in the lifetime of the application
     *
     * @param concurrency fixes up the concurrency in processing the messages
     * @param taskName    A descriptive name of the task that the handler will execute, to be used in log messages
     * @param task        - task represented as a Mono. Takes in the message as a parameter and take some action on
     *                    the message
     * @param clazz       - Type of message
     * @return pipeline which returns the output from the task
     */
    <V> void processMessage(int concurrency, String taskName, Function<T, Mono<V>> task, Class<T> clazz);

}

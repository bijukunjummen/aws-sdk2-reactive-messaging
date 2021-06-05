package org.bk.aws.messaging;

import reactor.core.publisher.Mono;

public class MessageWithDeleteHandle<T> {
    private final ContentMessage<T> message;

    // Represents computation to delete a message from an SQS queue
    // this is lazily invoked as part of the reactive streams subscription flow
    private final Mono<Void> deleteHandle;

    public MessageWithDeleteHandle(ContentMessage<T> message, Mono<Void> deleteHandle) {
        this.message = message;
        this.deleteHandle = deleteHandle;
    }

    public ContentMessage<T> getMessage() {
        return message;
    }

    public Mono<Void> getDeleteHandle() {
        return deleteHandle;
    }
}

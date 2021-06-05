package org.bk.aws.messaging;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a message as it flows through the system.
 * The body itself is immutable but the headers around it can potentially change.
 * The purpose is to store information like:
 * * start time that the message has come into the system and carry this
 * * message headers from sqs/sns
 * context as the message travels through the system.
 *
 * @param <T> represents the type of body
 */
public class ContentMessage<T> {
    private final Map<String, Object> headers;
    private final T body;

    public static final String CREATED_TIMESTAMP = "CREATED_TIMESTAMP";


    public ContentMessage(T body) {
        this.body = body;
        this.headers = new HashMap<>();
        this.headers.put(CREATED_TIMESTAMP, System.nanoTime());
    }

    public ContentMessage(T body, Map<String, Object> headers) {
        this(body);
        this.headers.putAll(headers);
    }

    public ContentMessage addHeader(String key, Object value) {
        this.headers.put(key, value);
        return this;
    }

    public T getBody() {
        return body;
    }

    public <V> V getHeader(String key) {
        return (V) this.headers.get(key);
    }

    public <V> ContentMessage<V> withNewBody(V newBody) {
        return new ContentMessage<>(newBody, this.headers);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ContentMessage)) {
            return false;
        }
        ContentMessage<?> message = (ContentMessage<?>) o;
        return Objects.equals(headers, message.headers) &&
                Objects.equals(body, message.body);
    }

    @Override
    public int hashCode() {
        return Objects.hash(headers, body);
    }

    @Override
    public String toString() {
        return "ContentMessage{" +
                "headers=" + headers +
                ", body=" + body +
                '}';
    }
}

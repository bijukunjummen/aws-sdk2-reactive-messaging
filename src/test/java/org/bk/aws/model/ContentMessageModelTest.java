package org.bk.aws.model;

import org.bk.aws.messaging.ContentMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ContentMessageModelTest {

    @Test
    void testContentMessage() {
        ContentMessage<String> msg = new ContentMessage<>("hello");
        assertThat(msg.getBody()).isEqualTo("hello");
        Long createdTs = msg.getHeader(ContentMessage.CREATED_TIMESTAMP);
        assertThat(createdTs).isNotNull();
        msg.addHeader("newHeaderKey", "newHeaderValue");
        assertThat((String) msg.getHeader("newHeaderKey")).isNotNull();
        assertThat(msg).isEqualTo(msg.withNewBody("hello"));
    }

    @Test
    void testResult() {
        Result<String> result = Result.of(() -> "hello");
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.isFailure()).isFalse();
        assertThat(result.get()).isEqualTo("hello");

        Result<String> failureResult = Result.of(() -> {
            throw new RuntimeException("Unexpected..");
        });

        assertThat(failureResult.isSuccess()).isFalse();
        assertThat(failureResult.isFailure()).isTrue();
        assertThat(failureResult.getCause()).isInstanceOf(RuntimeException.class);
    }
}

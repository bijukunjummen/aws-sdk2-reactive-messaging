package org.bk.aws.model;

/**
 * A type to hold the output of a computation. If a computation is successful, it returns the result, for failure it returns the cause of failure.
 *
 * To be used this way:
 * For a case where the computation is successful:

 * <pre>
 * Result result = Result.of(() -&gt; {
 *   return "hello";
 * });
 *
 * result.isSucess() == true;
 * </pre>
 *
 * For the case where the computation results in a failure:
 *
 * <pre>
 * Result result = Result.of(() -&gt; {
 *    throw new RuntimeException("failed");
 * });
 *
 *
 * result.isFailure() == true;
 * (result.getCause() instanceof Throwable) == true;
 * </pre>
 *
 * This is specifically created for the stream processing flow where it is important that any computation
 * not throw an exception as it then breaks out of the reactive streams pipeline, Result will wrap it up
 * to be handled in a subsequent step of the pipeline.
 *
 * @param <T> type of result
 */
public interface Result<T> {
    boolean isSuccess();

    boolean isFailure();

    T get();

    Throwable getCause();

    static <T> Result<T> success(T body) {
        return new Success<>(body);
    }

    static <T> Result<T> failure(Throwable t) {
        return new Failure<>(t);
    }

    static <T> Result<T> of(ThrowingSupplier<T, Exception> supplier) {
        try {
            return new Success<>(supplier.get());
        } catch (Throwable throwable) {
            return new Failure<>(throwable);
        }
    }


    class Success<T> implements Result<T> {
        private final T value;

        public Success(T value) {
            this.value = value;
        }

        @Override
        public boolean isSuccess() {
            return true;
        }

        @Override
        public boolean isFailure() {
            return false;
        }

        @Override
        public T get() {
            return value;
        }

        @Override
        public Throwable getCause() {
            return null;
        }
    }

    class Failure<T> implements Result<T> {
        private final Throwable cause;

        public Failure(Throwable cause) {
            this.cause = cause;
        }

        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public boolean isFailure() {
            return true;
        }

        @Override
        public T get() {
            throw new RuntimeException(this.getCause());
        }

        @Override
        public Throwable getCause() {
            return cause;
        }
    }


}

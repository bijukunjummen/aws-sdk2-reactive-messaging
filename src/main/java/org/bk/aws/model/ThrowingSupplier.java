package org.bk.aws.model;

/**
 * A version of {@link java.util.function.Supplier} that allows the body to throw a checked exception
 * without needing to have a try/catch block around it. Currently used by {@link Result}
 * @param <T> type to return
 * @param <E> type of exception
 */
@FunctionalInterface
public interface ThrowingSupplier<T, E extends Exception> {
    T get() throws E;
}

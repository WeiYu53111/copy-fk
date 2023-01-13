package flink.util;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/13/2023
 */

import javax.annotation.Nullable;

import static java.lang.String.format;

/** Utilities for working with {@link WrappingProxy}. */
//@Internal
public final class WrappingProxyUtil {

    //@VisibleForTesting
    static final int SAFETY_NET_MAX_ITERATIONS = 128;

    private WrappingProxyUtil() {
        throw new AssertionError();
    }

    /**
     * Expects a proxy, and returns the unproxied delegate.
     *
     * @param wrappingProxy The initial proxy.
     * @param <T> The type of the delegate. Note that all proxies in the chain must be assignable to
     *     T.
     * @return The unproxied delegate.
     */
    @SuppressWarnings("unchecked")
    public static <T> T stripProxy(@Nullable final WrappingProxy<T> wrappingProxy) {
        if (wrappingProxy == null) {
            return null;
        }

        T delegate = wrappingProxy.getWrappedDelegate();

        int numProxiesStripped = 0;
        while (delegate instanceof WrappingProxy) {
            throwIfSafetyNetExceeded(++numProxiesStripped);
            delegate = ((WrappingProxy<T>) delegate).getWrappedDelegate();
        }

        return delegate;
    }

    private static void throwIfSafetyNetExceeded(final int numProxiesStripped) {
        if (numProxiesStripped >= SAFETY_NET_MAX_ITERATIONS) {
            throw new IllegalArgumentException(
                    format(
                            "Already stripped %d proxies. "
                                    + "Are there loops in the object graph?",
                            SAFETY_NET_MAX_ITERATIONS));
        }
    }
}

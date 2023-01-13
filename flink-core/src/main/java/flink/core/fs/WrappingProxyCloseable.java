package flink.core.fs;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/10/2023
 */

import flink.util.WrappingProxy;

import java.io.Closeable;

/** {@link WrappingProxy} for {@link Closeable} that is also closeable. */
//@Internal
public interface WrappingProxyCloseable<T extends Closeable> extends Closeable, WrappingProxy<T> {}
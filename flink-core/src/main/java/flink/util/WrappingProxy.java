package flink.util;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/10/2023
 */

/**
 * Interface for objects that wrap another object and proxy (possibly a subset) of the methods of
 * that object.
 *
 * @param <T> The type that is wrapped.
 */
//@Internal
public interface WrappingProxy<T> {

    T getWrappedDelegate();
}

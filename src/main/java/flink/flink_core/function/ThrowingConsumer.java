package flink.flink_core.function;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/23/2022
 */
public interface ThrowingConsumer<T,E extends Throwable> {


    /**
     * Performs this operation on the given argument.
     *
     * @param t the input argument
     * @throws E on errors during consumption
     */
    void accept(T t) throws E;
}

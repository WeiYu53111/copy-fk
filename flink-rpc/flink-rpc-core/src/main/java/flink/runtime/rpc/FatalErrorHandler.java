package flink.runtime.rpc;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/21/2022
 */
public interface FatalErrorHandler {
    /**
     * Being called when a fatal error occurs.
     *
     * <p>IMPORTANT: This call should never be blocking since it might be called from within the
     * main thread of an {@link RpcEndpoint}.
     *
     * @param exception cause
     */
    void onFatalError(Throwable exception);


}

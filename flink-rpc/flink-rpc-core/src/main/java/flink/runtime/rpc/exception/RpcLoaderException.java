package flink.runtime.rpc.exception;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/11/2022
 */
public class RpcLoaderException extends RuntimeException{

    private static final long serialVersionUID = 7787884485642531050L;

    public RpcLoaderException(String message) {
        super(message);
    }

    public RpcLoaderException(Throwable cause) {
        super(cause);
    }

    public RpcLoaderException(String message, Throwable cause) {
        super(message, cause);
    }

}

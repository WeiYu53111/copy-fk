package rpc;

import java.util.concurrent.CompletableFuture;

/**
 * @Description
 * 通过 RpcService
 *  (1)启动rpcServ
 *  (2) 连接rpcServer, 获取到 rpcGateWay对象, 然后通过rpcGateWay对象 远程调用函数
 *
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/22/2022
 */
public interface RpcService {

    String getAddress();

    int getPort();

    /**
     * 连接指定地址的RpcServer 并返回 用于获取类型为 RpcGateWay子类的 CompletableFuture对象
     * @param address
     * @param clazz
     * @param <C>
     * @return
     */
    <C extends RpcGateway> CompletableFuture<C> connect(String address, Class<C> clazz);

}

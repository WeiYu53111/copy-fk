package flink.runtime.rpc;


import flink.configuration.Configuration;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/11/2022
 */
public interface RpcSystemLoader {
    RpcSystem loadRpcSystem(Configuration config);

}

package rpc;

import configuration.Configuration;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/22/2022
 */
public class RpcUtils {

    public static RpcService createRemoteRpcService(
            RpcSystem rpcSystem,
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange,
            @Nullable String bindAddress,
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<Integer> bindPort)
            throws Exception {
        RpcSystem.RpcServiceBuilder rpcServiceBuilder =
                rpcSystem.remoteServiceBuilder(configuration, externalAddress, externalPortRange);
        if (bindAddress != null) {
            rpcServiceBuilder = rpcServiceBuilder.withBindAddress(bindAddress);
        }
        if (bindPort.isPresent()) {
            rpcServiceBuilder = rpcServiceBuilder.withBindPort(bindPort.get());
        }
        return rpcServiceBuilder.createAndStart();
    }

}

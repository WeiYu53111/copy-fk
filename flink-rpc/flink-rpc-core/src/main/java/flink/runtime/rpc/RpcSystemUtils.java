package flink.runtime.rpc;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/20/2022
 */




import flink.configuration.Configuration;

import java.net.UnknownHostException;

/** Utils that are dependent on the underlying RPC implementation. */
public interface RpcSystemUtils {

    /**
     * Constructs an RPC URL for the given parameters, that can be used to connect to the targeted
     * RpcService.
     *
     * @param hostname The hostname or address where the target RPC service is listening.
     * @param port The port where the target RPC service is listening.
     * @param endpointName The name of the RPC endpoint.
     * @param addressResolution Whether to try address resolution of the given hostname or not. This
     *     allows to fail fast in case that the hostname cannot be resolved.
     * @param config The configuration from which to deduce further settings.
     * @return The RPC URL of the specified RPC endpoint.
     */
    String getRpcUrl(
            String hostname,
            int port,
            String endpointName,
            AddressResolution addressResolution,
            Configuration config)
            throws UnknownHostException;


}

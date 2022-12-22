package flink.flink_runtime.runtime.highavailability;

import flink.flink_core.api.java.tuple.Tuple2;
import flink.flink_core.configuration.Configuration;
import flink.flink_core.configuration.JobManagerOptions;
import flink.flink_core.configuration.RestOptions;
import flink.flink_core.configuration.SecurityOptions;
import flink.flink_rpc.flink_rpc_core.rpc.AddressResolution;
import flink.flink_rpc.flink_rpc_core.rpc.FatalErrorHandler;
import flink.flink_rpc.flink_rpc_core.rpc.RpcServiceUtils;
import flink.flink_rpc.flink_rpc_core.rpc.RpcSystemUtils;
import flink.flink_runtime.runtime.dispatcher.Dispatcher;
import flink.flink_runtime.runtime.highavailability.nonha.StandaloneHaServices;
import flink.flink_runtime.runtime.jobmanager.HighAvailabilityMode;
import flink.flink_runtime.runtime.resourcemanager.ResourceManager;

import javax.naming.ConfigurationException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executor;

import static flink.flink_core.util.Preconditions.checkNotNull;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/21/2022
 */
public class HighAvailabilityServicesUtils {

    public static HighAvailabilityServices createHighAvailabilityServices(
            Configuration configuration,
            Executor executor,
            AddressResolution addressResolution,
            RpcSystemUtils rpcSystemUtils,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {

        HighAvailabilityMode highAvailabilityMode = HighAvailabilityMode.fromConfig(configuration);

        switch (highAvailabilityMode) {
            case NONE:
                final Tuple2<String, Integer> hostnamePort = getJobManagerAddress(configuration);

                final String resourceManagerRpcUrl =
                        rpcSystemUtils.getRpcUrl(
                                hostnamePort.f0,
                                hostnamePort.f1,
                                RpcServiceUtils.createWildcardName(
                                        ResourceManager.RESOURCE_MANAGER_NAME),
                                addressResolution,
                                configuration);
                final String dispatcherRpcUrl =
                        rpcSystemUtils.getRpcUrl(
                                hostnamePort.f0,
                                hostnamePort.f1,
                                RpcServiceUtils.createWildcardName(Dispatcher.DISPATCHER_NAME),
                                addressResolution,
                                configuration);
                final String webMonitorAddress =
                        getWebMonitorAddress(configuration, addressResolution);

                return new StandaloneHaServices(
                        resourceManagerRpcUrl, dispatcherRpcUrl, webMonitorAddress);
            case ZOOKEEPER:
                /*BlobStoreService blobStoreService =
                        BlobUtils.createBlobStoreFromConfig(configuration);

                return new ZooKeeperHaServices(
                        ZooKeeperUtils.startCuratorFramework(configuration, fatalErrorHandler),
                        executor,
                        configuration,
                        blobStoreService);*/
                return null;

            case FACTORY_CLASS:
                //return createCustomHAServices(configuration, executor);
                return null;
            default:
                throw new Exception("Recovery mode " + highAvailabilityMode + " is not supported.");
        }
    }


    /**
     * Returns the JobManager's hostname and port extracted from the given {@link Configuration}.
     *
     * @param configuration Configuration to extract the JobManager's address from
     * @return The JobManager's hostname and port
     * @throws ConfigurationException if the JobManager's address cannot be extracted from the
     *     configuration
     */
    public static Tuple2<String, Integer> getJobManagerAddress(Configuration configuration)
            throws ConfigurationException {

        final String hostname = configuration.getString(JobManagerOptions.ADDRESS);
        final int port = configuration.getInteger(JobManagerOptions.PORT);

        if (hostname == null) {
            throw new ConfigurationException(
                    "Config parameter '"
                            + JobManagerOptions.ADDRESS
                            + "' is missing (hostname/address of JobManager to connect to).");
        }

        if (port <= 0 || port >= 65536) {
            throw new ConfigurationException(
                    "Invalid value for '"
                            + JobManagerOptions.PORT
                            + "' (port of the JobManager actor system) : "
                            + port
                            + ".  it must be greater than 0 and less than 65536.");
        }

        return Tuple2.of(hostname, port);
    }


    /**
     * Get address of web monitor from configuration.
     *
     * @param configuration Configuration contains those for WebMonitor.
     * @param resolution Whether to try address resolution of the given hostname or not. This allows
     *     to fail fast in case that the hostname cannot be resolved.
     * @return Address of WebMonitor.
     */
    public static String getWebMonitorAddress(
            Configuration configuration, AddressResolution resolution) throws UnknownHostException {
        final String address =
                checkNotNull(
                        configuration.getString(RestOptions.ADDRESS),
                        "%s must be set",
                        RestOptions.ADDRESS.key());

        if (resolution == AddressResolution.TRY_ADDRESS_RESOLUTION) {
            // Fail fast if the hostname cannot be resolved
            //noinspection ResultOfMethodCallIgnored
            InetAddress.getByName(address);
        }

        final int port = configuration.getInteger(RestOptions.PORT);
        final boolean enableSSL = SecurityOptions.isRestSSLEnabled(configuration);
        final String protocol = enableSSL ? "https://" : "http://";

        return String.format("%s%s:%s", protocol, address, port);
    }


}

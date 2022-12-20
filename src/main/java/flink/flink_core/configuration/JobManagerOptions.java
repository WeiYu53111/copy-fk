package flink.flink_core.configuration;

import static flink.flink_core.configuration.ConfigOptions.key;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/22/2022
 */
public class JobManagerOptions {
    public static final MemorySize MIN_JVM_HEAP_SIZE = MemorySize.ofMebiBytes(128);


    public static final ConfigOption<String> ADDRESS =
            key("jobmanager.flink.flink_rpc.flink.rpc.address")
                    .noDefaultValue()
                    .withDescription(
                            "The config parameter defining the network address to connect to"
                                    + " for communication with the job manager."
                                    + " This value is only interpreted in setups where a single JobManager with static"
                                    + " name or address exists (simple standalone setups, or container setups with dynamic"
                                    + " service name resolution). It is not used in many high-availability setups, when a"
                                    + " leader-election service (like ZooKeeper) is used to elect and discover the JobManager"
                                    + " leader from potentially multiple standby JobManagers.");

    public static final ConfigOption<Integer> PORT =
            key("jobmanager.flink.flink_rpc.flink.rpc.port")
                    .defaultValue(6123)
                    .withDescription(
                            "The config parameter defining the network port to connect to"
                                    + " for communication with the job manager."
                                    + " Like "
                                    + ADDRESS.key()
                                    + ", this value is only interpreted in setups where"
                                    + " a single JobManager with static name/address and port exists (simple standalone setups,"
                                    + " or container setups with dynamic service name resolution)."
                                    + " This config option is not used in many high-availability setups, when a"
                                    + " leader-election service (like ZooKeeper) is used to elect and discover the JobManager"
                                    + " leader from potentially multiple standby JobManagers.");


    /** The local address of the network interface that the job manager binds to. */
    public static final ConfigOption<String> BIND_HOST =
            key("jobmanager.bind-host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The local address of the network interface that the job manager binds to. If not"
                                    + " configured, '0.0.0.0' will be used.");

    /** The local port that the job manager binds to. */
    public static final ConfigOption<Integer> RPC_BIND_PORT =
            key("jobmanager.flink.flink_rpc.flink.rpc.bind-port")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The local RPC port that the JobManager binds to. If not configured, the external port"
                                    + " (configured by '"
                                    + PORT.key()
                                    + "') will be used.");


}

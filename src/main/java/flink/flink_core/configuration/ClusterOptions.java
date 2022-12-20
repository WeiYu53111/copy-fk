package flink.flink_core.configuration;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/20/2022
 */
/** Options which control the cluster behaviour. */
public class ClusterOptions {

    // TODO 生成文档的注解 到底是怎么工作的？ @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Long> INITIAL_REGISTRATION_TIMEOUT =
            ConfigOptions.key("cluster.registration.initial-timeout")
                    .defaultValue(100L)
                    .withDescription(
                            "Initial registration timeout between cluster components in milliseconds.");

    // @Documentation.Section(Documentation.Sections.EXPERT_FAULT_TOLERANCE)
    public static final ConfigOption<Integer> CLUSTER_IO_EXECUTOR_POOL_SIZE =
            ConfigOptions.key("cluster.io-pool.size")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The size of the IO executor pool used by the cluster to execute blocking IO operations (Master as well as TaskManager processes). "
                                    + "By default it will use 4 * the number of CPU cores (hardware contexts) that the cluster process has access to. "
                                    + "Increasing the pool size allows to run more IO operations concurrently.");

}

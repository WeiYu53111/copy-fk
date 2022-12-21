package flink.flink_runtime.runtime.entrypoint;

import flink.flink_core.configuration.ClusterOptions;
import flink.flink_core.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flink.flink_runtime.runtime.util.Hardware;
import flink.flink_core.util.Preconditions;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/20/2022
 */
public class ClusterEntrypointUtils {


    protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypointUtils.class);

    private ClusterEntrypointUtils() {
        throw new UnsupportedOperationException("This class should not be instantiated.");
    }


    /**
     * Gets and verify the io-executor pool size based on flink.flink_core.configuration.
     *
     * @param config The flink.flink_core.configuration to read.
     * @return The legal io-executor pool size.
     */
    public static int getPoolSize(Configuration config) {
        final int poolSize =
                config.getInteger(
                        ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE,
                        4 * Hardware.getNumberCPUCores());
        Preconditions.checkArgument(
                poolSize > 0,
                String.format(
                        "Illegal pool size (%s) of io-executor, please re-configure '%s'.",
                        poolSize, ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE.key()));
        return poolSize;
    }
}

package flink.runtime.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/20/2022
 */
public class Hardware {

    private static final Logger LOG = LoggerFactory.getLogger(Hardware.class);

    /**
     * Gets the number of CPU cores (hardware contexts) that the JVM has access to.
     *
     * @return The number of CPU cores.
     */
    public static int getNumberCPUCores() {
        return Runtime.getRuntime().availableProcessors();
    }
}

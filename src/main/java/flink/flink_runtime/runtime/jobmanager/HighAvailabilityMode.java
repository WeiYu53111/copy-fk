package flink.flink_runtime.runtime.jobmanager;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/21/2022
 */


import flink.flink_core.configuration.ConfigConstants;
import flink.flink_core.configuration.Configuration;
import flink.flink_core.configuration.HighAvailabilityOptions;

/**
 * High availability mode for Flink's cluster execution. Currently supported modes are:
 *
 * <p>- NONE: No high availability. - ZooKeeper: JobManager high availability via ZooKeeper
 * ZooKeeper is used to select a leader among a group of JobManager. This JobManager is responsible
 * for the job execution. Upon failure of the leader a new leader is elected which will take over
 * the responsibilities of the old leader - FACTORY_CLASS: Use implementation of {@link
 * org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory} specified in
 * configuration property high-availability
 */
public enum HighAvailabilityMode {

    NONE(false),
    ZOOKEEPER(true),
    FACTORY_CLASS(true);

    private final boolean haActive;

    HighAvailabilityMode(boolean haActive) {
        this.haActive = haActive;
    }

    /**
     * Return the configured {@link HighAvailabilityMode}.
     *
     * @param config The config to parse
     * @return Configured recovery mode or {@link HighAvailabilityMode#NONE} if not configured.
     */
    public static HighAvailabilityMode fromConfig(Configuration config) {
        String haMode = config.getValue(HighAvailabilityOptions.HA_MODE);

        if (haMode == null) {
            return HighAvailabilityMode.NONE;
        } else if (haMode.equalsIgnoreCase(ConfigConstants.DEFAULT_RECOVERY_MODE)) {
            // Map old default to new default
            return HighAvailabilityMode.NONE;
        } else {
            try {
                return HighAvailabilityMode.valueOf(haMode.toUpperCase());
            } catch (IllegalArgumentException e) {
                return FACTORY_CLASS;
            }
        }
    }
}

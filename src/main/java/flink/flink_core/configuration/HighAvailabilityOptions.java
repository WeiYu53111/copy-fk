package flink.flink_core.configuration;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/21/2022
 */


import static flink.flink_core.configuration.ConfigOptions.key;

/** The set of configuration options relating to high-availability settings. */
public class HighAvailabilityOptions {


    public static final ConfigOption<String> HA_MODE =
            key("high-availability")
                    .defaultValue("NONE")
                    .withDeprecatedKeys("recovery.mode")
                    .withDescription(
                            "Defines high-availability mode used for the cluster execution."
                                    + " To enable high-availability, set this mode to \"ZOOKEEPER\" or specify FQN of factory class.");
}

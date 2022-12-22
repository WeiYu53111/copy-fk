package flink.flink_core.configuration;

import static flink.flink_core.configuration.ConfigOptions.key;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class WebOptions {


    /** Config parameter defining the runtime monitor web-frontend server address. */
    @Deprecated
    public static final ConfigOption<String> ADDRESS =
            key("web.address")
                    .noDefaultValue()
                    .withDeprecatedKeys("jobmanager.web.address")
                    .withDescription("Address for runtime monitor web-frontend server.");

    /**
     * The port for the runtime monitor web-frontend server.
     *
     * @deprecated Use {@link RestOptions#PORT} instead
     */
    @Deprecated
    public static final ConfigOption<Integer> PORT =
            key("web.port").defaultValue(8081).withDeprecatedKeys("jobmanager.web.port");
}

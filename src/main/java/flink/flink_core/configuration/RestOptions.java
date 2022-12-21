package flink.flink_core.configuration;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/21/2022
 */
public class RestOptions {

    private static final String REST_PORT_KEY = "rest.port";

    public static final ConfigOption<String> ADDRESS =
            key("rest.address")
                    .noDefaultValue()
                    .withFallbackKeys(JobManagerOptions.ADDRESS.key())
                    .withDescription(
                            "The address that should be used by clients to connect to the server. Attention: This option is respected only if the high-availability configuration is NONE.");


}

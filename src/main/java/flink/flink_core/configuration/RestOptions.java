package flink.flink_core.configuration;

import flink.flink_core.configuration.description.Description;

import static flink.flink_core.configuration.ConfigOptions.key;
import static flink.flink_core.configuration.description.TextElement.text;

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


    public static final ConfigOption<String> BIND_PORT =
            key("rest.bind-port")
                    .defaultValue("8081")
                    .withFallbackKeys(REST_PORT_KEY)
                    .withDeprecatedKeys(
                            WebOptions.PORT.key(), ConfigConstants.JOB_MANAGER_WEB_PORT_KEY)
                    .withDescription(
                            "The port that the server binds itself. Accepts a list of ports (“50100,50101”), ranges"
                                    + " (“50100-50200”) or a combination of both. It is recommended to set a range of ports to avoid"
                                    + " collisions when multiple Rest servers are running on the same machine.");



    public static final ConfigOption<Integer> PORT =
            key(REST_PORT_KEY)
                    .defaultValue(8081)
                    .withDeprecatedKeys(WebOptions.PORT.key())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The port that the client connects to. If %s has not been specified, then the REST server will bind to this port. Attention: This option is respected only if the high-availability configuration is NONE.",
                                            text(BIND_PORT.key()))
                                    .build());



}

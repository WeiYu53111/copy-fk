package flink.flink_core.configuration;

import static flink.flink_core.configuration.ConfigOptions.key;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class SecurityOptions {


    /**
     * Enable SSL for internal (rpc, data transport, blob server) and external (HTTP/REST)
     * communication.
     *
     * @deprecated Use {@link #SSL_INTERNAL_ENABLED} and {@link #SSL_REST_ENABLED} instead.
     */
    @Deprecated
    public static final ConfigOption<Boolean> SSL_ENABLED =
            key("security.ssl.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Turns on SSL for internal and external network communication."
                                    + "This can be overridden by 'security.ssl.internal.enabled', 'security.ssl.external.enabled'. "
                                    + "Specific internal components (rpc, data transport, blob server) may optionally override "
                                    + "this through their own settings.");


    public static final ConfigOption<Boolean> SSL_REST_ENABLED =
            key("security.ssl.rest.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Turns on SSL for external communication via the REST endpoints.");


    /** Checks whether SSL for the external REST endpoint is enabled. */
    public static boolean isRestSSLEnabled(Configuration sslConfig) {
        @SuppressWarnings("deprecation")
        final boolean fallbackFlag = sslConfig.getBoolean(SSL_ENABLED);
        return sslConfig.getBoolean(SSL_REST_ENABLED, fallbackFlag);
    }


    /**
     * Checks whether SSL for internal communication (rpc, data transport, blob server) is enabled.
     */
    public static boolean isInternalSSLEnabled(Configuration sslConfig) {
        @SuppressWarnings("deprecation")
        final boolean fallbackFlag = sslConfig.getBoolean(SSL_ENABLED);
        return sslConfig.getBoolean(SSL_INTERNAL_ENABLED, fallbackFlag);
    }


    /** Enable SSL for internal communication (akka rpc, netty data transport, blob server). */
    public static final ConfigOption<Boolean> SSL_INTERNAL_ENABLED =
            key("security.ssl.internal.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Turns on SSL for internal network communication. "
                                    + "Optionally, specific components may override this through their own settings "
                                    + "(rpc, data transport, REST, etc).");

}

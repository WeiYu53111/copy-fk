package flink.configuration;



import flink.configuration.description.Description;

import static flink.configuration.ConfigOptions.key;
import static flink.configuration.description.LinkElement.link;


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

    public static final ConfigOption<String> SSL_PROTOCOL =
            key("security.ssl.protocol")
                    .stringType()
                    .defaultValue("TLSv1.2")
                    .withDescription(
                            "The SSL protocol version to be supported for the ssl transport. Note that it doesn’t"
                                    + " support comma separated list.");



    /**
     * The standard SSL algorithms to be supported.
     *
     * <p>More options here -
     * http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites
     */
    public static final ConfigOption<String> SSL_ALGORITHMS =
            key("security.ssl.algorithms")
                    .stringType()
                    .defaultValue("TLS_RSA_WITH_AES_128_CBC_SHA")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The comma separated list of standard SSL algorithms to be supported. Read more %s",
                                            link(
                                                    "http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites",
                                                    "here"))
                                    .build());



    /** SSL session cache size. */
    public static final ConfigOption<Integer> SSL_INTERNAL_SESSION_CACHE_SIZE =
            key("security.ssl.internal.session-cache-size")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The size of the cache used for storing SSL session objects. "
                                                    + "According to %s, you should always set "
                                                    + "this to an appropriate number to not run into a bug with stalling IO threads "
                                                    + "during garbage collection. (-1 = use system default).",
                                            link(
                                                    "https://github.com/netty/netty/issues/832",
                                                    "here"))
                                    .build())
                    .withDeprecatedKeys("security.ssl.session-cache-size");

    /** SSL session timeout. */
    public static final ConfigOption<Integer> SSL_INTERNAL_SESSION_TIMEOUT =
            key("security.ssl.internal.session-timeout")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The timeout (in ms) for the cached SSL session objects. (-1 = use system default)")
                    .withDeprecatedKeys("security.ssl.session-timeout");



    /** For internal SSL, the Java keystore file containing the private key and certificate. */
    public static final ConfigOption<String> SSL_INTERNAL_KEYSTORE =
            key("security.ssl.internal.keystore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Java keystore file with SSL Key and Certificate, "
                                    + "to be used Flink's internal endpoints (rpc, data transport, blob server).");



    /**
     * For external (REST) SSL, the Java keystore file containing the private key and certificate.
     */
    public static final ConfigOption<String> SSL_REST_KEYSTORE =
            key("security.ssl.rest.keystore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Java keystore file with SSL Key and Certificate, "
                                    + "to be used Flink's external REST endpoints.");


    /**
     * For internal SSL, the truststore file containing the public CA certificates to verify the ssl
     * peers.
     */
    public static final ConfigOption<String> SSL_INTERNAL_TRUSTSTORE =
            key("security.ssl.internal.truststore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The truststore file containing the public CA certificates to verify the peer "
                                    + "for Flink's internal endpoints (rpc, data transport, blob server).");



    /**
     * For external (REST) SSL, the truststore file containing the public CA certificates to verify
     * the ssl peers.
     */
    public static final ConfigOption<String> SSL_REST_TRUSTSTORE =
            key("security.ssl.rest.truststore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The truststore file containing the public CA certificates to verify the peer "
                                    + "for Flink's external REST endpoints.");


    /** The Java keystore file containing the flink endpoint key and certificate. */
    public static final ConfigOption<String> SSL_KEYSTORE =
            key("security.ssl.keystore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Java keystore file to be used by the flink endpoint for its SSL Key and Certificate.");


    /** For internal SSL, the password to decrypt the keystore file containing the certificate. */
    public static final ConfigOption<String> SSL_INTERNAL_KEYSTORE_PASSWORD =
            key("security.ssl.internal.keystore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The secret to decrypt the keystore file for Flink's "
                                    + "for Flink's internal endpoints (rpc, data transport, blob server).");


    /**
     * For external (REST) SSL, the password to decrypt the keystore file containing the
     * certificate.
     */
    public static final ConfigOption<String> SSL_REST_KEYSTORE_PASSWORD =
            key("security.ssl.rest.keystore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The secret to decrypt the keystore file for Flink's "
                                    + "for Flink's external REST endpoints.");



    /** Secret to decrypt the server key. */
    public static final ConfigOption<String> SSL_KEY_PASSWORD =
            key("security.ssl.key-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The secret to decrypt the server key in the keystore.");

    /** The truststore file containing the public CA certificates to verify the ssl peers. */
    public static final ConfigOption<String> SSL_TRUSTSTORE =
            key("security.ssl.truststore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The truststore file containing the public CA certificates to be used by flink endpoints"
                                    + " to verify the peer’s certificate.");

    /** Secret to decrypt the truststore. */
    public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD =
            key("security.ssl.truststore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The secret to decrypt the truststore.");


    /** Secret to decrypt the keystore file. */
    public static final ConfigOption<String> SSL_KEYSTORE_PASSWORD =
            key("security.ssl.keystore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The secret to decrypt the keystore file.");


    /** For internal SSL, the secret to decrypt the truststore. */
    public static final ConfigOption<String> SSL_INTERNAL_TRUSTSTORE_PASSWORD =
            key("security.ssl.internal.truststore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The password to decrypt the truststore "
                                    + "for Flink's internal endpoints (rpc, data transport, blob server).");


    /** For internal SSL, the sha1 fingerprint of the internal certificate to verify the client. */
    public static final ConfigOption<String> SSL_INTERNAL_CERT_FINGERPRINT =
            key("security.ssl.internal.cert.fingerprint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The sha1 fingerprint of the internal certificate. "
                                    + "This further protects the internal communication to present the exact certificate used by Flink."
                                    + "This is necessary where one cannot use private CA(self signed) or there is internal firm wide CA is required");



    /** For internal SSL, the password to decrypt the private key. */
    public static final ConfigOption<String> SSL_INTERNAL_KEY_PASSWORD =
            key("security.ssl.internal.key-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The secret to decrypt the key in the keystore "
                                    + "for Flink's internal endpoints (rpc, data transport, blob server).");




    /** For external (REST) SSL, the secret to decrypt the truststore. */
    public static final ConfigOption<String> SSL_REST_TRUSTSTORE_PASSWORD =
            key("security.ssl.rest.truststore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The password to decrypt the truststore "
                                    + "for Flink's external REST endpoints.");


    /** For external (REST) SSL, the password to decrypt the private key. */
    public static final ConfigOption<String> SSL_REST_KEY_PASSWORD =
            key("security.ssl.rest.key-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The secret to decrypt the key in the keystore "
                                    + "for Flink's external REST endpoints.");



    /** For external (REST) SSL, the sha1 fingerprint of the rest client certificate to verify. */
    public static final ConfigOption<String> SSL_REST_CERT_FINGERPRINT =
            key("security.ssl.rest.cert.fingerprint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The sha1 fingerprint of the rest certificate. "
                                    + "This further protects the rest REST endpoints to present certificate which is only used by proxy server"
                                    + "This is necessary where once uses public CA or internal firm wide CA");


}

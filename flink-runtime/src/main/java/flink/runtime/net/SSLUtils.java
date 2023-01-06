package flink.runtime.net;


import flink.configuration.ConfigOption;
import flink.configuration.Configuration;
import flink.configuration.IllegalConfigurationException;
import flink.configuration.SecurityOptions;
import flink.util.StringUtils;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.*;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.util.FingerprintTrustManagerFactory;

import javax.annotation.Nullable;
import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.List;


import static flink.util.Preconditions.checkNotNull;
import static org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslProvider.*;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class SSLUtils {


    /**
     * Creates a factory for SSL Client Sockets from the given configuration. SSL Client Sockets are
     * always part of internal communication.
     */
    public static SocketFactory createSSLClientSocketFactory(Configuration config)
            throws Exception {
        SSLContext sslContext = createInternalSSLContext(config, true);
        if (sslContext == null) {
            throw new IllegalConfigurationException("SSL is not enabled");
        }

        return sslContext.getSocketFactory();
    }

    /**
     * Creates a factory for SSL Server Sockets from the given configuration. SSL Server Sockets are
     * always part of internal communication.
     */
    public static ServerSocketFactory createSSLServerSocketFactory(Configuration config)
            throws Exception {
        SSLContext sslContext = createInternalSSLContext(config, false);
        if (sslContext == null) {
            throw new IllegalConfigurationException("SSL is not enabled");
        }

        String[] protocols = getEnabledProtocols(config);
        String[] cipherSuites = getEnabledCipherSuites(config);

        SSLServerSocketFactory factory = sslContext.getServerSocketFactory();
        return new ConfiguringSSLServerSocketFactory(factory, protocols, cipherSuites);
    }

    private static String[] getEnabledProtocols(final Configuration config) {
        checkNotNull(config, "config must not be null");
        return config.getString(SecurityOptions.SSL_PROTOCOL).split(",");
    }

    private static String[] getEnabledCipherSuites(final Configuration config) {
        checkNotNull(config, "config must not be null");
        return config.getString(SecurityOptions.SSL_ALGORITHMS).split(",");
    }

    /**
     * Creates the SSL Context for internal SSL, if internal SSL is configured. For internal SSL,
     * the client and server side configuration are identical, because of mutual authentication.
     */
    @Nullable
    private static SSLContext createInternalSSLContext(Configuration config, boolean clientMode)
            throws Exception {
        JdkSslContext nettySSLContext =
                (JdkSslContext) createInternalNettySSLContext(config, clientMode, JDK);
        if (nettySSLContext != null) {
            return nettySSLContext.context();
        } else {
            return null;
        }
    }



    /**
     * Creates the SSL Context for internal SSL, if internal SSL is configured. For internal SSL,
     * the client and server side configuration are identical, because of mutual authentication.
     */
    @Nullable
    private static SslContext createInternalNettySSLContext(
            Configuration config, boolean clientMode, SslProvider provider) throws Exception {
        checkNotNull(config, "config");

        if (!SecurityOptions.isInternalSSLEnabled(config)) {
            return null;
        }

        String[] sslProtocols = getEnabledProtocols(config);
        List<String> ciphers = Arrays.asList(getEnabledCipherSuites(config));
        int sessionCacheSize = config.getInteger(SecurityOptions.SSL_INTERNAL_SESSION_CACHE_SIZE);
        int sessionTimeoutMs = config.getInteger(SecurityOptions.SSL_INTERNAL_SESSION_TIMEOUT);

        KeyManagerFactory kmf = getKeyManagerFactory(config, true, provider);
        TrustManagerFactory tmf = getTrustManagerFactory(config, true);
        ClientAuth clientAuth = ClientAuth.REQUIRE;

        final SslContextBuilder sslContextBuilder;
        if (clientMode) {
            sslContextBuilder = SslContextBuilder.forClient().keyManager(kmf);
        } else {
            sslContextBuilder = SslContextBuilder.forServer(kmf);
        }

        return sslContextBuilder
                .sslProvider(provider)
                .protocols(sslProtocols)
                .ciphers(ciphers)
                .trustManager(tmf)
                .clientAuth(clientAuth)
                .sessionCacheSize(sessionCacheSize)
                .sessionTimeout(sessionTimeoutMs / 1000)
                .build();
    }

    private static class ConfiguringSSLServerSocketFactory extends ServerSocketFactory{

        private final SSLServerSocketFactory sslServerSocketFactory;
        private final String[] protocols;
        private final String[] cipherSuites;

        ConfiguringSSLServerSocketFactory(
                SSLServerSocketFactory sslServerSocketFactory,
                String[] protocols,
                String[] cipherSuites) {

            this.sslServerSocketFactory = sslServerSocketFactory;
            this.protocols = protocols;
            this.cipherSuites = cipherSuites;
        }


        @Override
        public ServerSocket createServerSocket(int port) throws IOException {
            SSLServerSocket socket =
                    (SSLServerSocket) sslServerSocketFactory.createServerSocket(port);
            configureServerSocket(socket);
            return socket;
        }

        @Override
        public ServerSocket createServerSocket(int port, int backlog) throws IOException {
            SSLServerSocket socket =
                    (SSLServerSocket) sslServerSocketFactory.createServerSocket(port, backlog);
            configureServerSocket(socket);
            return socket;
        }

        @Override
        public ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress)
                throws IOException {
            SSLServerSocket socket =
                    (SSLServerSocket)
                            sslServerSocketFactory.createServerSocket(port, backlog, ifAddress);
            configureServerSocket(socket);
            return socket;
        }

        private void configureServerSocket(SSLServerSocket socket) {
            socket.setEnabledProtocols(protocols);
            socket.setEnabledCipherSuites(cipherSuites);
            socket.setNeedClientAuth(true);
        }
    }



    private static KeyManagerFactory getKeyManagerFactory(
            Configuration config, boolean internal, SslProvider provider)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
            UnrecoverableKeyException {
        String keystoreFilePath =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_KEYSTORE
                                : SecurityOptions.SSL_REST_KEYSTORE,
                        SecurityOptions.SSL_KEYSTORE);

        String keystorePassword =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD
                                : SecurityOptions.SSL_REST_KEYSTORE_PASSWORD,
                        SecurityOptions.SSL_KEYSTORE_PASSWORD);

        String certPassword =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_KEY_PASSWORD
                                : SecurityOptions.SSL_REST_KEY_PASSWORD,
                        SecurityOptions.SSL_KEY_PASSWORD);

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream keyStoreFile = Files.newInputStream(new File(keystoreFilePath).toPath())) {
            keyStore.load(keyStoreFile, keystorePassword.toCharArray());
        }

        final KeyManagerFactory kmf;
        if (provider == OPENSSL || provider == OPENSSL_REFCNT) {
            kmf = new OpenSslX509KeyManagerFactory();
        } else {
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        }
        kmf.init(keyStore, certPassword.toCharArray());

        return kmf;
    }

    private static TrustManagerFactory getTrustManagerFactory(
            Configuration config, boolean internal)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        String trustStoreFilePath =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_TRUSTSTORE
                                : SecurityOptions.SSL_REST_TRUSTSTORE,
                        SecurityOptions.SSL_TRUSTSTORE);

        String trustStorePassword =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD
                                : SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD,
                        SecurityOptions.SSL_TRUSTSTORE_PASSWORD);

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream trustStoreFile =
                     Files.newInputStream(new File(trustStoreFilePath).toPath())) {
            trustStore.load(trustStoreFile, trustStorePassword.toCharArray());
        }

        String certFingerprint =
                config.getString(
                        internal
                                ? SecurityOptions.SSL_INTERNAL_CERT_FINGERPRINT
                                : SecurityOptions.SSL_REST_CERT_FINGERPRINT);

        TrustManagerFactory tmf;
        if (StringUtils.isNullOrWhitespaceOnly(certFingerprint)) {
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        } else {
            tmf = new FingerprintTrustManagerFactory(certFingerprint.split(","));
        }

        tmf.init(trustStore);

        return tmf;
    }


    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static String getAndCheckOption(
            Configuration config,
            ConfigOption<String> primaryOption,
            ConfigOption<String> fallbackOption) {
        String value = config.getString(primaryOption, config.getString(fallbackOption));
        if (value != null) {
            return value;
        } else {
            throw new IllegalConfigurationException(
                    "The config option "
                            + primaryOption.key()
                            + " or "
                            + fallbackOption.key()
                            + " is missing.");
        }
    }


}

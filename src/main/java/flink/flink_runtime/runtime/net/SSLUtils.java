package flink.flink_runtime.runtime.net;

import flink.flink_core.configuration.Configuration;
import flink.flink_core.configuration.IllegalConfigurationException;

import javax.annotation.Nullable;
import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class SSLUtils {



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


}

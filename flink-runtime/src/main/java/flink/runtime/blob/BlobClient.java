package flink.runtime.blob;

import flink.api.common.JobID;
import flink.configuration.BlobServerOptions;
import flink.configuration.Configuration;
import flink.configuration.SecurityOptions;
import flink.core.fs.Path;
import flink.runtime.net.SSLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static flink.util.Preconditions.checkNotNull;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/6/2023
 */
public class BlobClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(BlobClient.class);

    /** The socket connection to the BLOB server. */
    private final Socket socket;

    /**
     * Instantiates a new BLOB client.
     *
     * @param serverAddress the network address of the BLOB server
     * @param clientConfig additional configuration like SSL parameters required to connect to the
     *     blob server
     * @throws IOException thrown if the connection to the BLOB server could not be established
     */
    public BlobClient(InetSocketAddress serverAddress, Configuration clientConfig)
            throws IOException {
        Socket socket = null;

        try {
            // create an SSL socket if configured
            if (SecurityOptions.isInternalSSLEnabled(clientConfig)
                    && clientConfig.getBoolean(BlobServerOptions.SSL_ENABLED)) {
                LOG.info("Using ssl connection to the blob server");

                socket = SSLUtils.createSSLClientSocketFactory(clientConfig).createSocket();
            } else {
                socket = new Socket();
            }

            // Establish the socket using the hostname and port. This avoids a potential issue where
            // the
            // InetSocketAddress can cache a failure in hostname resolution forever.
            socket.connect(
                    new InetSocketAddress(serverAddress.getHostName(), serverAddress.getPort()),
                    clientConfig.getInteger(BlobServerOptions.CONNECT_TIMEOUT));
            socket.setSoTimeout(clientConfig.getInteger(BlobServerOptions.SO_TIMEOUT));
        } catch (Exception e) {
            BlobUtils.closeSilently(socket, LOG);
            throw new IOException("Could not connect to BlobServer at address " + serverAddress, e);
        }

        this.socket = socket;
    }




    /**
     * Uploads the JAR files to the {@link PermanentBlobService} of the {@link BlobServer} at the
     * given address with HA as configured.
     *
     * @param serverAddress Server address of the {@link BlobServer}
     * @param clientConfig Any additional configuration for the blob client
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param files List of files to upload
     * @throws IOException if the upload fails
     */
    public static List<PermanentBlobKey> uploadFiles(
            InetSocketAddress serverAddress,
            Configuration clientConfig,
            JobID jobId,
            List<Path> files)
            throws IOException {

        checkNotNull(jobId);

        if (files.isEmpty()) {
            return Collections.emptyList();
        } else {
            List<PermanentBlobKey> blobKeys = new ArrayList<>();

            try (BlobClient blobClient = new BlobClient(serverAddress, clientConfig)) {
                for (final Path file : files) {
                    final PermanentBlobKey key = blobClient.uploadFile(jobId, file);
                    blobKeys.add(key);
                }
            }

            return blobKeys;
        }
    }


    /**
     * Uploads a single file to the {@link PermanentBlobService} of the given {@link BlobServer}.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param file file to upload
     * @throws IOException if the upload fails
     */
    public PermanentBlobKey uploadFile(JobID jobId, Path file) throws IOException {
        final FileSystem fs = file.getFileSystem();
        try (InputStream is = fs.open(file)) {
            return (PermanentBlobKey) putInputStream(jobId, is, PERMANENT_BLOB);
        }
    }


    @Override
    public void close() throws IOException {

    }
}

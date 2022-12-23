package flink.flink_runtime.runtime.blob;

import flink.flink_core.configuration.BlobServerOptions;
import flink.flink_core.configuration.Configuration;
import flink.flink_core.configuration.ConfigurationUtils;
import flink.flink_core.util.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Random;
import java.util.UUID;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class BlobUtils {


    private static final Random RANDOM = new Random();

    /**
     * Creates a local storage directory for a blob service under the configuration parameter given
     * by {@link BlobServerOptions#STORAGE_DIRECTORY}. If this is <tt>null</tt> or empty, we will
     * fall back to Flink's temp directories (given by {@link
     * org.apache.flink.configuration.CoreOptions#TMP_DIRS}) and choose one among them at random.
     *
     * @param config Flink configuration
     * @return a new local storage directory
     * @throws IOException thrown if the local file storage cannot be created or is not usable
     */
    static File initLocalStorageDirectory(Configuration config) throws IOException {

        String basePath = config.getString(BlobServerOptions.STORAGE_DIRECTORY);

        File baseDir;
        if (StringUtils.isNullOrWhitespaceOnly(basePath)) {
            final String[] tmpDirPaths = ConfigurationUtils.parseTempDirectories(config);
            baseDir = new File(tmpDirPaths[RANDOM.nextInt(tmpDirPaths.length)]);
        } else {
            baseDir = new File(basePath);
        }

        File storageDir;

        // NOTE: although we will be using UUIDs, there may be collisions
        int maxAttempts = 10;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            storageDir =
                    new File(baseDir, String.format("blobStore-%s", UUID.randomUUID().toString()));

            // Create the storage dir if it doesn't exist. Only return it when the operation was
            // successful.
            if (storageDir.mkdirs()) {
                return storageDir;
            }
        }

        // max attempts exceeded to find a storage directory
        throw new IOException(
                "Could not create storage directory for BLOB store in '" + baseDir + "'.");
    }

    static void closeSilently(Socket socket, Logger log) {
        if (socket != null) {
            try {
                socket.close();
            } catch (Throwable t) {
                log.debug("Exception while closing BLOB server connection socket.", t);
            }
        }
    }


}

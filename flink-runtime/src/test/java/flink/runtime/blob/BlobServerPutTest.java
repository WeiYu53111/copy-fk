package flink.runtime.blob;


import flink.api.common.JobID;
import flink.configuration.BlobServerOptions;
import flink.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static flink.runtime.blob.BlobKeyTest.verifyKeyDifferentHashEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/3/2023
 */
public class BlobServerPutTest {

    private final Random rnd = new Random();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    /**
     * Uploads two byte arrays for different jobs into the server via the {@link BlobServer}. File
     * transfers should be successful.
     *
     * @param jobId1 first job id
     * @param jobId2 second job id
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testPutBufferSuccessfulGet(
            @Nullable JobID jobId1, @Nullable JobID jobId2, BlobKey.BlobType blobType)
            throws IOException {

        final Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);
            byte[] data2 = Arrays.copyOfRange(data, 10, 54);

            // put data for jobId1 and verify
            BlobKey key1a = put(server, jobId1, data, blobType);
            assertNotNull(key1a);
            // second upload of same data should yield a different BlobKey
            BlobKey key1a2 = put(server, jobId1, data, blobType);
            assertNotNull(key1a2);
            verifyKeyDifferentHashEquals(key1a, key1a2);

            BlobKey key1b = put(server, jobId1, data2, blobType);
            assertNotNull(key1b);

            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);

            // now put data for jobId2 and verify that both are ok
            BlobKey key2a = put(server, jobId2, data, blobType);
            assertNotNull(key2a);
            verifyKeyDifferentHashEquals(key1a, key2a);

            BlobKey key2b = put(server, jobId2, data2, blobType);
            assertNotNull(key2b);
            verifyKeyDifferentHashEquals(key1b, key2b);

            // verify the accessibility and the BLOB contents
            verifyContents(server, jobId2, key2a, data);
            verifyContents(server, jobId2, key2b, data2);

            // verify the accessibility and the BLOB contents one more time (transient BLOBs should
            // not be deleted here)
            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);
            verifyContents(server, jobId2, key2a, data);
            verifyContents(server, jobId2, key2b, data2);
        }
    }


    @Test
    public void testPutBufferSuccessfulGet1() throws IOException {
        testPutBufferSuccessfulGet(null, null, TRANSIENT_BLOB);
    }



    /**
     * Helper to choose the right {@link BlobServer#putTransient} method.
     *
     * @param blobType whether the BLOB should become permanent or transient
     * @return blob key for the uploaded data
     */
    static BlobKey put(
            BlobService service, @Nullable JobID jobId, byte[] data, BlobKey.BlobType blobType)
            throws IOException {
        if (blobType == PERMANENT_BLOB) {
            if (service instanceof BlobServer) {
                return ((BlobServer) service).putPermanent(jobId, data);
            } else {
                // implement via JAR file upload instead:
                File tmpFile = Files.createTempFile("blob", ".jar").toFile();
                try {
                    FileUtils.writeByteArrayToFile(tmpFile, data);
                    InetSocketAddress serverAddress =
                            new InetSocketAddress("localhost", service.getPort());
                    // uploading HA BLOBs works on BlobServer only (and, for now, via the
                    // BlobClient)
                    Configuration clientConfig = new Configuration();
                    List<Path> jars =
                            Collections.singletonList(new Path(tmpFile.getAbsolutePath()));
                    List<PermanentBlobKey> keys =
                            BlobClient.uploadFiles(serverAddress, clientConfig, jobId, jars);
                    assertEquals(1, keys.size());
                    return keys.get(0);
                } finally {
                    //noinspection ResultOfMethodCallIgnored
                    tmpFile.delete();
                }
            }
        } else if (jobId == null) {
            return service.getTransientBlobService().putTransient(data);
        } else {
            return service.getTransientBlobService().putTransient(jobId, data);
        }
    }



    /**
     * GET the data stored at the two keys and check that it is equal to <tt>data</tt>.
     *
     * @param blobService BlobServer to use
     * @param jobId job ID or <tt>null</tt> if job-unrelated
     * @param key blob key
     * @param data expected data
     */
    static void verifyContents(
            BlobService blobService, @Nullable JobID jobId, BlobKey key, byte[] data)
            throws IOException {

        File file = get(blobService, jobId, key);
        validateGetAndClose(new FileInputStream(file), data);
    }

}

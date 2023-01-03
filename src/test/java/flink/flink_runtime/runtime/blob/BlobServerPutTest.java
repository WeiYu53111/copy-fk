package flink.flink_runtime.runtime.blob;

import flink.flink_core.api.common.JobID;
import flink.flink_core.configuration.BlobServerOptions;
import flink.flink_core.configuration.Configuration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/3/2023
 */
public class BlobServerPutTest {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
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


}

package flink.runtime.blob;

import flink.api.common.JobID;
import flink.util.TestLogger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/4/2023
 */



/**
 * Tests how failing GET requests behave in the presence of failures when used with a {@link
 * BlobServer}.
 *
 * <p>Successful GET requests are tested in conjunction wit the PUT requests by {@link
 * BlobServerPutTest}.
 */
public class BlobServerGetTest extends TestLogger {


    /**
     * Retrieves the given blob.
     *
     * <p>Note that if a {@link BlobCacheService} is used, it may try to access the {@link
     * BlobServer} to retrieve the blob.
     *
     * @param service BLOB client to use for connecting to the BLOB service
     * @param jobId job ID or <tt>null</tt> if job-unrelated
     * @param key key identifying the BLOB to request
     */
    static File get(BlobService service, @Nullable JobID jobId, BlobKey key) throws IOException {
        if (key instanceof PermanentBlobKey) {
            return service.getPermanentBlobService().getFile(jobId, (PermanentBlobKey) key);
        } else if (jobId == null) {
            return service.getTransientBlobService().getFile((TransientBlobKey) key);
        } else {
            return service.getTransientBlobService().getFile(jobId, (TransientBlobKey) key);
        }
    }

}

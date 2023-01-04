package flink.runtime.blob;

import flink.api.common.JobID;
import flink.util.FileUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public interface PermanentBlobService extends Closeable {

    /**
     * Returns the path to a local copy of the file associated with the provided job ID and blob
     * key.
     *
     * @param jobId ID of the job this blob belongs to
     * @param key BLOB key associated with the requested file
     * @return The path to the file.
     * @throws java.io.FileNotFoundException if the BLOB does not exist;
     * @throws IOException if any other error occurs when retrieving the file
     */
    File getFile(JobID jobId, PermanentBlobKey key) throws IOException;

    /**
     * Returns the content of the file for the BLOB with the provided job ID the blob key.
     *
     * <p>Compared to {@code getFile}, {@code readFile} will attempt to read the entire file after
     * retrieving it. If file reading and file retrieving is done in the same WRITE lock, it can
     * avoid the scenario that the path to the file is deleted concurrently by other threads when
     * the file is retrieved but not read yet.
     *
     * @param jobId ID of the job this blob belongs to
     * @param key BLOB key associated with the requested file
     * @return The content of the BLOB.
     * @throws java.io.FileNotFoundException if the BLOB does not exist;
     * @throws IOException if any other error occurs when retrieving the file.
     */
    default byte[] readFile(JobID jobId, PermanentBlobKey key) throws IOException {
        // The default implementation doesn't guarantee that the file won't be deleted concurrently
        // by other threads while reading the contents.
        return FileUtils.readAllBytes(getFile(jobId, key).toPath());
    }

}

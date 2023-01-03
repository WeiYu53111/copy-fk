package flink.flink_runtime.runtime.blob;

import flink.flink_core.api.common.JobID;

import java.io.File;
import java.io.IOException;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public interface BlobStore  extends BlobView{

    /**
     * Copies the local file to the blob store.
     *
     * @param localFile The file to copy
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param blobKey The ID for the file in the blob store
     * @return whether the file was copied (<tt>true</tt>) or not (<tt>false</tt>)
     * @throws IOException If the copy fails
     */
    boolean put(File localFile, JobID jobId, BlobKey blobKey) throws IOException;

    /**
     * Tries to delete a blob from storage.
     *
     * <p>NOTE: This also tries to delete any created directories if empty.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param blobKey The blob ID
     * @return <tt>true</tt> if the given blob is successfully deleted or non-existing;
     *     <tt>false</tt> otherwise
     */
    boolean delete(JobID jobId, BlobKey blobKey);

    /**
     * Tries to delete all blobs for the given job from storage.
     *
     * <p>NOTE: This also tries to delete any created directories if empty.
     *
     * @param jobId The JobID part of all blobs to delete
     * @return <tt>true</tt> if the job directory is successfully deleted or non-existing;
     *     <tt>false</tt> otherwise
     */
    boolean deleteAll(JobID jobId);


}

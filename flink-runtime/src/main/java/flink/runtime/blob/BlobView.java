package flink.runtime.blob;


import flink.api.common.JobID;

import java.io.File;
import java.io.IOException;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/3/2023
 */
public interface BlobView {


    /**
     * Copies a blob to a local file.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param blobKey The blob ID
     * @param localFile The local file to copy to
     * @return whether the file was copied (<tt>true</tt>) or not (<tt>false</tt>)
     * @throws IOException If the copy fails
     */
    boolean get(JobID jobId, BlobKey blobKey, File localFile) throws IOException;

}

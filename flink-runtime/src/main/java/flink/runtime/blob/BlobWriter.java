package flink.runtime.blob;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/4/2023
 */


import flink.api.common.JobID;

import java.io.IOException;
import java.io.InputStream;

/** BlobWriter is used to upload data to the BLOB store. */
public interface BlobWriter {


    /**
     * Uploads the data of the given byte array for the given job to the BLOB server and makes it a
     * permanent BLOB.
     *
     * @param jobId the ID of the job the BLOB belongs to
     * @param value the buffer to upload
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while writing it to a local file, or
     *     uploading it to the HA store
     */
    PermanentBlobKey putPermanent(JobID jobId, byte[] value) throws IOException;

    /**
     * Uploads the data from the given input stream for the given job to the BLOB server and makes
     * it a permanent BLOB.
     *
     * @param jobId ID of the job this blob belongs to
     * @param inputStream the input stream to read the data from
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while reading the data from the input
     *     stream, writing it to a local file, or uploading it to the HA store
     */
    PermanentBlobKey putPermanent(JobID jobId, InputStream inputStream) throws IOException;

}

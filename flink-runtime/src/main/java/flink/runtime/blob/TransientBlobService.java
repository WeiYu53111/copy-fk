package flink.runtime.blob;

import flink.api.common.JobID;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public interface TransientBlobService extends AutoCloseable{


    // --------------------------------------------------------------------------------------------
    //  GET
    // --------------------------------------------------------------------------------------------

    /**
     * Returns the path to a local copy of the (job-unrelated) file associated with the provided
     * blob key.
     *
     * @param key blob key associated with the requested file
     * @return The path to the file.
     * @throws java.io.FileNotFoundException when the path does not exist;
     * @throws IOException if any other error occurs when retrieving the file
     */
    File getFile(TransientBlobKey key) throws IOException;

    /**
     * Returns the path to a local copy of the file associated with the provided job ID and blob
     * key.
     *
     * @param jobId ID of the job this blob belongs to
     * @param key blob key associated with the requested file
     * @return The path to the file.
     * @throws java.io.FileNotFoundException when the path does not exist;
     * @throws IOException if any other error occurs when retrieving the file
     */
    File getFile(JobID jobId, TransientBlobKey key) throws IOException;

    // --------------------------------------------------------------------------------------------
    //  PUT
    // --------------------------------------------------------------------------------------------

    /**
     * Uploads the (job-unrelated) data of the given byte array to the BLOB server.
     *
     * @param value the buffer to upload
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while uploading the data to the BLOB server
     */
    TransientBlobKey putTransient(byte[] value) throws IOException;

    /**
     * Uploads the data of the given byte array for the given job to the BLOB server.
     *
     * @param jobId the ID of the job the BLOB belongs to
     * @param value the buffer to upload
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while uploading the data to the BLOB server
     */
    TransientBlobKey putTransient(JobID jobId, byte[] value) throws IOException;

    /**
     * Uploads the (job-unrelated) data from the given input stream to the BLOB server.
     *
     * @param inputStream the input stream to read the data from
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while reading the data from the input
     *     stream or uploading the data to the BLOB server
     */
    TransientBlobKey putTransient(InputStream inputStream) throws IOException;

    /**
     * Uploads the data from the given input stream for the given job to the BLOB server.
     *
     * @param jobId ID of the job this blob belongs to
     * @param inputStream the input stream to read the data from
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while reading the data from the input
     *     stream or uploading the data to the BLOB server
     */
    TransientBlobKey putTransient(JobID jobId, InputStream inputStream) throws IOException;

    // --------------------------------------------------------------------------------------------
    //  DELETE
    // --------------------------------------------------------------------------------------------

    /**
     * Deletes the (job-unrelated) file associated with the provided blob key from the local cache.
     *
     * @param key associated with the file to be deleted
     * @return <tt>true</tt> if the given blob is successfully deleted or non-existing;
     *     <tt>false</tt> otherwise
     */
    boolean deleteFromCache(TransientBlobKey key);

    /**
     * Deletes the file associated with the provided job ID and blob key from the local cache.
     *
     * @param jobId ID of the job this blob belongs to
     * @param key associated with the file to be deleted
     * @return <tt>true</tt> if the given blob is successfully deleted or non-existing;
     *     <tt>false</tt> otherwise
     */
    boolean deleteFromCache(JobID jobId, TransientBlobKey key);

}

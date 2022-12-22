package flink.flink_runtime.runtime.highavailability;

import flink.flink_runtime.runtime.blob.BlobStore;

import java.io.IOException;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/20/2022
 */
public interface HighAvailabilityServices {


    /**
     * Creates the BLOB store in which BLOBs are stored in a highly-available fashion.
     *
     * @return Blob store
     * @throws IOException if the blob store could not be created
     */
    BlobStore createBlobStore() throws IOException;

}

package flink.runtime.blob;

import java.io.Closeable;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public interface BlobService  extends Closeable {


    /**
     * Returns a BLOB service for accessing permanent BLOBs.
     *
     * @return BLOB service
     */
    PermanentBlobService getPermanentBlobService();

    /**
     * Returns a BLOB service for accessing transient BLOBs.
     *
     * @return BLOB service
     */
    TransientBlobService getTransientBlobService();

    /**
     * Returns the port of the BLOB server that this BLOB service is working with.
     *
     * @return the port of the blob server.
     */
    int getPort();

}

package flink.flink_runtime.runtime.blob;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/3/2023
 */

/** BLOB key referencing permanent BLOB files. */
public class PermanentBlobKey extends BlobKey {


    /** Constructs a new BLOB key. */
    // TODO @VisibleForTesting
    public PermanentBlobKey() {
        super(BlobType.PERMANENT_BLOB);
    }

    /**
     * Constructs a new BLOB key from the given byte array.
     *
     * @param key the actual key data
     */
    PermanentBlobKey(byte[] key) {
        super(BlobType.PERMANENT_BLOB, key);
    }

    /**
     * Constructs a new BLOB key from the given byte array.
     *
     * @param key the actual key data
     * @param random the random component of the key
     */
    PermanentBlobKey(byte[] key, byte[] random) {
        super(BlobType.PERMANENT_BLOB, key, random);
    }
}

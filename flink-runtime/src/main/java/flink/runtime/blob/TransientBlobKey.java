package flink.runtime.blob;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class TransientBlobKey extends BlobKey{


    /** Constructs a new BLOB key. */
    public TransientBlobKey() {
        super(BlobType.TRANSIENT_BLOB);
    }



    /**
     * Constructs a new BLOB key from the given byte array.
     *
     * @param key the actual key data
     */
    TransientBlobKey(byte[] key) {
        super(BlobType.TRANSIENT_BLOB, key);
    }

    /**
     * Constructs a new BLOB key from the given byte array.
     *
     * @param key the actual key data
     * @param random the random component of the key
     */
    TransientBlobKey(byte[] key, byte[] random) {
        super(BlobType.TRANSIENT_BLOB, key, random);
    }

}

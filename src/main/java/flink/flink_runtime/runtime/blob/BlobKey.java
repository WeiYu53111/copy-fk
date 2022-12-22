package flink.flink_runtime.runtime.blob;

import flink.flink_core.util.AbstractID;

import java.io.Serializable;

import static flink.flink_core.util.Preconditions.checkNotNull;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 *
 *
 * 唯一标识一个Blob
 */
public abstract class BlobKey implements Serializable, Comparable<BlobKey>{
    private static final long serialVersionUID = 3847117712521785209L;


    /** Size of the internal BLOB key in bytes. */
    public static final int SIZE = 20;

    /** The byte buffer storing the actual key data. */
    private final byte[] key;

    /** (Internal) BLOB type - to be reflected by the inheriting sub-class. */
    private final BlobType type;

    /** BLOB type, i.e. permanent or transient. */
    enum BlobType {
        /**
         * Indicates a permanent BLOB whose lifecycle is that of a job and which is made highly
         * available.
         */
        PERMANENT_BLOB,
        /**
         * Indicates a transient BLOB whose lifecycle is managed by the user and which is not made
         * highly available.
         */
        TRANSIENT_BLOB
    }


    /** Random component of the key. */
    private final AbstractID random;

    /**
     * Constructs a new BLOB key.
     *
     * @param type whether the referenced BLOB is permanent or transient
     */
    protected BlobKey(BlobType type) {
        this.type = checkNotNull(type);
        this.key = new byte[SIZE];
        this.random = new AbstractID();
    }

    /**
     * Constructs a new BLOB key from the given byte array.
     *
     * @param type whether the referenced BLOB is permanent or transient
     * @param key the actual key data
     */
    protected BlobKey(BlobType type, byte[] key) {
        if (key == null || key.length != SIZE) {
            throw new IllegalArgumentException("BLOB key must have a size of " + SIZE + " bytes");
        }

        this.type = checkNotNull(type);
        this.key = key;
        this.random = new AbstractID();
    }

    /**
     * Constructs a new BLOB key from the given byte array.
     *
     * @param type whether the referenced BLOB is permanent or transient
     * @param key the actual key data
     * @param random the random component of the key
     */
    protected BlobKey(BlobType type, byte[] key, byte[] random) {
        if (key == null || key.length != SIZE) {
            throw new IllegalArgumentException("BLOB key must have a size of " + SIZE + " bytes");
        }

        this.type = checkNotNull(type);
        this.key = key;
        this.random = new AbstractID(random);
    }



    @Override
    public int compareTo(BlobKey o) {
        // compare the hashes first
        final byte[] aarr = this.key;
        final byte[] barr = o.key;
        final int len = Math.min(aarr.length, barr.length);

        for (int i = 0; i < len; ++i) {
            final int a = (aarr[i] & 0xff);
            final int b = (barr[i] & 0xff);
            if (a != b) {
                return a - b;
            }
        }

        if (aarr.length == barr.length) {
            // same hash contents - compare the BLOB types
            int typeCompare = this.type.compareTo(o.type);
            if (typeCompare == 0) {
                // same type - compare random components
                return this.random.compareTo(o.random);
            } else {
                return typeCompare;
            }
        } else {
            return aarr.length - barr.length;
        }
    }
}
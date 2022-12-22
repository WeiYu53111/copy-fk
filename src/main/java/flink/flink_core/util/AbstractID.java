package flink.flink_core.util;

import java.util.Random;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class AbstractID implements Comparable<AbstractID>, java.io.Serializable{

    private static final long serialVersionUID = 1L;

    /** The upper part of the actual ID. */
    protected final long upperPart;

    /** The lower part of the actual ID. */
    protected final long lowerPart;

    private static final Random RND = new Random();


    /** The size of a long in bytes. */
    private static final int SIZE_OF_LONG = 8;

    /** The size of the ID in byte. */
    public static final int SIZE = 2 * SIZE_OF_LONG;


    /** Constructs a new random ID from a uniform distribution. */
    public AbstractID() {
        this.lowerPart = RND.nextLong();
        this.upperPart = RND.nextLong();
    }



    /** Constructs a new ID with a specific bytes value. */
    public AbstractID(byte[] bytes) {
        if (bytes == null || bytes.length != SIZE) {
            throw new IllegalArgumentException(
                    "Argument bytes must by an array of " + SIZE + " bytes");
        }

        this.lowerPart = byteArrayToLong(bytes, 0);
        this.upperPart = byteArrayToLong(bytes, SIZE_OF_LONG);
    }

    /**
     * Constructs a new abstract ID.
     *
     * @param lowerPart the lower bytes of the ID
     * @param upperPart the higher bytes of the ID
     */
    public AbstractID(long lowerPart, long upperPart) {
        this.lowerPart = lowerPart;
        this.upperPart = upperPart;
    }

    @Override
    public int compareTo(AbstractID o) {
        int diff1 = Long.compare(this.upperPart, o.upperPart);
        int diff2 = Long.compare(this.lowerPart, o.lowerPart);
        return diff1 == 0 ? diff2 : diff1;
    }



    /**
     * Converts the given byte array to a long.
     *
     * @param ba the byte array to be converted
     * @param offset the offset indicating at which byte inside the array the conversion shall begin
     * @return the long variable
     */
    private static long byteArrayToLong(byte[] ba, int offset) {
        long l = 0;

        for (int i = 0; i < SIZE_OF_LONG; ++i) {
            l |= (ba[offset + SIZE_OF_LONG - 1 - i] & 0xffL) << (i << 3);
        }

        return l;
    }


}

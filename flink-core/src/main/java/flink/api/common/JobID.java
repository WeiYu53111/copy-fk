package flink.api.common;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */


import flink.util.AbstractID;

/**
 * Unique (at least statistically unique) identifier for a Flink Job. Jobs in Flink correspond to
 * dataflow graphs.
 *
 * <p>Jobs act simultaneously as <i>sessions</i>, because jobs can be created and submitted
 * incrementally in different parts. Newer fragments of a graph can be attached to existing graphs,
 * thereby extending the current data flow graphs.
 *
 *
 * 唯一标识一个flink job
 */
public class JobID  extends AbstractID {

    /**
     * Creates a new JobID, using the given lower and upper parts.
     *
     * @param lowerPart The lower 8 bytes of the ID.
     * @param upperPart The upper 8 bytes of the ID.
     */
    public JobID(long lowerPart, long upperPart) {
        super(lowerPart, upperPart);
    }


    /**
     * Creates a new JobID from the given byte sequence. The byte sequence must be exactly 16 bytes
     * long. The first eight bytes make up the lower part of the ID, while the next 8 bytes make up
     * the upper part of the ID.
     *
     * @param bytes The byte sequence.
     */
    public JobID(byte[] bytes) {
        super(bytes);
    }


    /**
     * Creates a new JobID from the given byte sequence. The byte sequence must be exactly 16 bytes
     * long. The first eight bytes make up the lower part of the ID, while the next 8 bytes make up
     * the upper part of the ID.
     *
     * @param bytes The byte sequence.
     * @return A new JobID corresponding to the ID encoded in the bytes.
     */
    public static JobID fromByteArray(byte[] bytes) {
        return new JobID(bytes);
    }

}

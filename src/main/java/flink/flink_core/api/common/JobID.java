package flink.flink_core.api.common;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */


import flink.flink_core.util.AbstractID;

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
}

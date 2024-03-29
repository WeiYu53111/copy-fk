package flink.core.fs;

import java.io.IOException;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/13/2023
 */



/** A BlockLocation lists hosts, offset and length of block. */
//@Public
public interface BlockLocation extends Comparable<BlockLocation>{

    /**
     * Get the list of hosts (hostname) hosting this block.
     *
     * @return A list of hosts (hostname) hosting this block.
     * @throws IOException thrown if the list of hosts could not be retrieved
     */
    String[] getHosts() throws IOException;

    /**
     * Get the start offset of the file associated with this block.
     *
     * @return The start offset of the file associated with this block.
     */
    long getOffset();

    /**
     * Get the length of the block.
     *
     * @return the length of the block
     */
    long getLength();
}

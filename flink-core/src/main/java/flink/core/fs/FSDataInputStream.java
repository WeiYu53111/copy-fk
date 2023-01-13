package flink.core.fs;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/13/2023
 */


import java.io.IOException;
import java.io.InputStream;

/**
 * Interface for a data input stream to a file on a {@link FileSystem}.
 *
 * <p>This extends the {@link java.io.InputStream} with methods for accessing the stream's {@link
 * #getPos() current position} and {@link #seek(long) seeking} to a desired position.
 */
@Public
public abstract class FSDataInputStream extends InputStream {

    /**
     * Seek to the given offset from the start of the file. The next read() will be from that
     * location. Can't seek past the end of the stream.
     *
     * @param desired the desired offset
     * @throws IOException Thrown if an error occurred while seeking inside the input stream.
     */
    public abstract void seek(long desired) throws IOException;

    /**
     * Gets the current position in the input stream.
     *
     * @return current position in the input stream
     * @throws IOException Thrown if an I/O error occurred in the underlying stream implementation
     *     while accessing the stream's position.
     */
    public abstract long getPos() throws IOException;
}


package flink.core.fs;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/13/2023
 */

import java.io.IOException;


import flink.core.fs.RecoverableWriter.ResumeRecoverable;
import flink.core.fs.RecoverableWriter.CommitRecoverable;

/**
 * An output stream to a file system that can be recovered at well defined points. The stream
 * initially writes to hidden files or temp files and only creates the target file once it is closed
 * and "committed".
 */
//@PublicEvolving
public abstract class RecoverableFsDataOutputStream extends FSDataOutputStream {

    /**
     * Ensures all data so far is persistent (similar to {@link #sync()}) and returns a handle to
     * recover the stream at the current position.
     */
    public abstract ResumeRecoverable persist() throws IOException;

    /**
     * Closes the stream, ensuring persistence of all data (similar to {@link #sync()}). This
     * returns a Committer that can be used to publish (make visible) the file that the stream was
     * writing to.
     */
    public abstract Committer closeForCommit() throws IOException;

    /**
     * Closes this stream. Closing the steam releases the local resources that the stream uses, but
     * does NOT result in durability of previously written data. This method should be interpreted
     * as a "close in order to dispose" or "close on failure".
     *
     * <p>In order to persist all previously written data, one needs to call the {@link
     * #closeForCommit()} method and call {@link Committer#commit()} on the retured committer
     * object.
     *
     * @throws IOException Thrown if an error occurred during closing.
     */
    @Override
    public abstract void close() throws IOException;

    // ------------------------------------------------------------------------

    /**
     * A committer can publish the file of a stream that was closed. The Committer can be recovered
     * via a {@link CommitRecoverable}.
     */
    public interface Committer {

        /**
         * Commits the file, making it visible. The file will contain the exact data as when the
         * committer was created.
         *
         * @throws IOException Thrown if committing fails.
         */
        void commit() throws IOException;

        /**
         * Commits the file, making it visible. The file will contain the exact data as when the
         * committer was created.
         *
         * <p>This method tolerates situations where the file was already committed and will not
         * raise an exception in that case. This is important for idempotent commit retries as they
         * need to happen after recovery.
         *
         * @throws IOException Thrown if committing fails.
         */
        void commitAfterRecovery() throws IOException;

        /**
         * Gets a recoverable object to recover the committer. The recovered committer will commit
         * the file with the exact same data as this committer would commit it.
         */
        CommitRecoverable getRecoverable();
    }
}

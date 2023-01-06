package flink.core.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/6/2023
 */
public abstract class FileSystem {



    /**
     * The possible write modes. The write mode decides what happens if a file should be created,
     * but already exists.
     */
    public enum WriteMode {

        /**
         * Creates the target file only if no file exists at that path already. Does not overwrite
         * existing files and directories.
         */
        NO_OVERWRITE,

        /**
         * Creates a new target file regardless of any existing files or directories. Existing files
         * and directories will be deleted (recursively) automatically before creating the new file.
         */
        OVERWRITE
    }



    /** Logger for all FileSystem work. */
    private static final Logger LOG = LoggerFactory.getLogger(FileSystem.class);

}

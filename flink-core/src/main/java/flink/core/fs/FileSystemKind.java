package flink.core.fs;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/13/2023
 */

/** An enumeration defining the kind and characteristics of a {@link FileSystem}. */
//@PublicEvolving
public enum FileSystemKind {

    /** An actual file system, with files and directories. */
    FILE_SYSTEM,

    /**
     * An Object store. Files correspond to objects. There are not really directories, but a
     * directory-like structure may be mimicked by hierarchical naming of files.
     */
    OBJECT_STORE
}


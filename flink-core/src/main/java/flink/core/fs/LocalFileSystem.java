package flink.core.fs;

import flink.util.OperatingSystem;

import java.net.URI;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/9/2023
 */
public class LocalFileSystem extends FileSystem{


    /** The URI representing the local file system. */
    private static final URI LOCAL_URI =
            OperatingSystem.isWindows() ? URI.create("file:/") : URI.create("file:///");

    /** The shared instance of the local file system. */
    private static final LocalFileSystem INSTANCE = new LocalFileSystem();


    /**
     * Gets the URI that represents the local file system. That URI is {@code "file:/"} on Windows
     * platforms and {@code "file:///"} on other UNIX family platforms.
     *
     * @return The URI that represents the local file system.
     */
    public static URI getLocalFsURI() {
        return LOCAL_URI;
    }

    /**
     * Gets the shared instance of this file system.
     *
     * @return The shared instance of this file system.
     */
    public static LocalFileSystem getSharedInstance() {
        return INSTANCE;
    }
}

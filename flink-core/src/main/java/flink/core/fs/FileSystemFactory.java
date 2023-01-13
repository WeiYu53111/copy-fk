package flink.core.fs;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/13/2023
 */

import flink.core.plugin.Plugin;

import java.io.IOException;
import java.net.URI;

/**
 * A factory to create file systems.
 *
 * <p>The factory is typically configured via {@link #configure(Configuration)} before creating file
 * systems via {@link #create(URI)}.
 */
//@PublicEvolving
public interface FileSystemFactory extends Plugin {

    /** Gets the scheme of the file system created by this factory. */
    String getScheme();

    /**
     * Creates a new file system for the given file system URI. The URI describes the type of file
     * system (via its scheme) and optionally the authority (for example the host) of the file
     * system.
     *
     * @param fsUri The URI that describes the file system.
     * @return A new instance of the specified file system.
     * @throws IOException Thrown if the file system could not be instantiated.
     */
    FileSystem create(URI fsUri) throws IOException;
}
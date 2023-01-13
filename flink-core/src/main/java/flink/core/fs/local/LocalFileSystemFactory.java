package flink.core.fs.local;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/13/2023
 */

import flink.core.fs.FileSystem;
import flink.core.fs.FileSystemFactory;
import flink.core.fs.LocalFileSystem;

import java.net.URI;

/** A factory for the {@link LocalFileSystem}. */
//@PublicEvolving
public class LocalFileSystemFactory implements FileSystemFactory {

    @Override
    public String getScheme() {
        return LocalFileSystem.getLocalFsURI().getScheme();
    }

    @Override
    public FileSystem create(URI fsUri) {
        return LocalFileSystem.getSharedInstance();
    }
}

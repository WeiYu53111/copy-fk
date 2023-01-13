package flink.core.fs;

import flink.configuration.Configuration;
import flink.configuration.IllegalConfigurationException;
import flink.core.fs.local.LocalFileSystemFactory;
import flink.core.plugin.PluginManager;
import flink.util.ExceptionUtils;
import flink.util.TemporaryClassLoaderContext;
import org.apache.flink.shaded.guava30.com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static flink.util.Preconditions.checkNotNull;

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


    /** Cache for file systems, by scheme + authority. */
    private static final HashMap<FSKey, FileSystem> CACHE = new HashMap<>();


    /**
     * Mapping of file system schemes to the corresponding factories, populated in {@link
     * FileSystem#initialize(Configuration, PluginManager)}.
     */
    private static final HashMap<String, FileSystemFactory> FS_FACTORIES = new HashMap<>();

    /** Object used to protect calls to specific methods. */
    private static final ReentrantLock LOCK = new ReentrantLock(true);

    /** Logger for all FileSystem work. */
    private static final Logger LOG = LoggerFactory.getLogger(FileSystem.class);


    /**
     * The default filesystem scheme to be used, configured during process-wide initialization. This
     * value defaults to the local file systems scheme {@code 'file:///'} or {@code 'file:/'}.
     */
    private static URI defaultScheme;

    /**
     * Returns a reference to the {@link FileSystem} instance for accessing the file system
     * identified by the given {@link URI}.
     *
     * @param uri the {@link URI} identifying the file system
     * @return a reference to the {@link FileSystem} instance for accessing the file system
     *     identified by the given {@link URI}.
     * @throws IOException thrown if a reference to the file system instance could not be obtained
     */
    public static FileSystem get(URI uri) throws IOException {
        return FileSystemSafetyNet.wrapWithSafetyNetWhenActivated(getUnguardedFileSystem(uri));
    }


    /**
     * Gets the default file system URI that is used for paths and file systems that do not specify
     * and explicit scheme.
     *
     * <p>As an example, assume the default file system URI is set to {@code
     * 'hdfs://someserver:9000/'}. A file path of {@code '/user/USERNAME/in.txt'} is interpreted as
     * {@code 'hdfs://someserver:9000/user/USERNAME/in.txt'}.
     *
     * @return The default file system URI
     */
    public static URI getDefaultFsUri() {
        return defaultScheme != null ? defaultScheme : LocalFileSystem.getLocalFsURI();
    }

    private static void initializeWithoutPlugins(Configuration config)
            throws IllegalConfigurationException {
        initialize(config, null);
    }


    /**
     * Initializes the shared file system settings.
     *
     * <p>The given configuration is passed to each file system factory to initialize the respective
     * file systems. Because the configuration of file systems may be different subsequent to the
     * call of this method, this method clears the file system instance cache.
     *
     * <p>This method also reads the default file system URI from the configuration key {@link
     * CoreOptions#DEFAULT_FILESYSTEM_SCHEME}. All calls to {@link FileSystem#get(URI)} where the
     * URI has no scheme will be interpreted as relative to that URI. As an example, assume the
     * default file system URI is set to {@code 'hdfs://localhost:9000/'}. A file path of {@code
     * '/user/USERNAME/in.txt'} is interpreted as {@code
     * 'hdfs://localhost:9000/user/USERNAME/in.txt'}.
     *
     * @param config the configuration from where to fetch the parameter.
     * @param pluginManager optional plugin manager that is used to initialized filesystems provided
     *     as plugins.
     */
    public static void initialize(Configuration config, PluginManager pluginManager)
            throws IllegalConfigurationException {

        LOCK.lock();
        try {
            // make sure file systems are re-instantiated after re-configuration
            CACHE.clear();
            FS_FACTORIES.clear();

            Collection<Supplier<Iterator<FileSystemFactory>>> factorySuppliers = new ArrayList<>(2);
            factorySuppliers.add(() -> ServiceLoader.load(FileSystemFactory.class).iterator());

            if (pluginManager != null) {
                factorySuppliers.add(
                        () ->
                                Iterators.transform(
                                        pluginManager.load(FileSystemFactory.class),
                                        PluginFileSystemFactory::of));
            }

            final List<FileSystemFactory> fileSystemFactories =
                    loadFileSystemFactories(factorySuppliers);

            // configure all file system factories
            for (FileSystemFactory factory : fileSystemFactories) {
                factory.configure(config);
                String scheme = factory.getScheme();

                FileSystemFactory fsf =
                        ConnectionLimitingFactory.decorateIfLimited(factory, scheme, config);
                FS_FACTORIES.put(scheme, fsf);
            }

            // configure the default (fallback) factory
            FALLBACK_FACTORY.configure(config);

            // also read the default file system scheme
            final String stringifiedUri =
                    config.getString(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, null);
            if (stringifiedUri == null) {
                defaultScheme = null;
            } else {
                try {
                    defaultScheme = new URI(stringifiedUri);
                } catch (URISyntaxException e) {
                    throw new IllegalConfigurationException(
                            "The default file system scheme ('"
                                    + CoreOptions.DEFAULT_FILESYSTEM_SCHEME
                                    + "') is invalid: "
                                    + stringifiedUri,
                            e);
                }
            }

            ALLOWED_FALLBACK_FILESYSTEMS.clear();
            final Iterable<String> allowedFallbackFilesystems =
                    Splitter.on(';')
                            .omitEmptyStrings()
                            .trimResults()
                            .split(config.getString(CoreOptions.ALLOWED_FALLBACK_FILESYSTEMS));
            allowedFallbackFilesystems.forEach(ALLOWED_FALLBACK_FILESYSTEMS::add);
        } finally {
            LOCK.unlock();
        }
    }


    //@Internal
    public static FileSystem getUnguardedFileSystem(final URI fsUri) throws IOException {
        checkNotNull(fsUri, "file system URI");

        LOCK.lock();
        try {
            final URI uri;

            if (fsUri.getScheme() != null) {
                uri = fsUri;
            } else {
                // Apply the default fs scheme
                final URI defaultUri = getDefaultFsUri();
                URI rewrittenUri = null;

                try {
                    rewrittenUri =
                            new URI(
                                    defaultUri.getScheme(),
                                    null,
                                    defaultUri.getHost(),
                                    defaultUri.getPort(),
                                    fsUri.getPath(),
                                    null,
                                    null);
                } catch (URISyntaxException e) {
                    // for local URIs, we make one more try to repair the path by making it absolute
                    if (defaultUri.getScheme().equals("file")) {
                        try {
                            rewrittenUri =
                                    new URI(
                                            "file",
                                            null,
                                            new Path(new File(fsUri.getPath()).getAbsolutePath())
                                                    .toUri()
                                                    .getPath(),
                                            null);
                        } catch (URISyntaxException ignored) {
                            // could not help it...
                        }
                    }
                }

                if (rewrittenUri != null) {
                    uri = rewrittenUri;
                } else {
                    throw new IOException(
                            "The file system URI '"
                                    + fsUri
                                    + "' declares no scheme and cannot be interpreted relative to the default file system URI ("
                                    + defaultUri
                                    + ").");
                }
            }

            // print a helpful pointer for malformed local URIs (happens a lot to new users)
            if (uri.getScheme().equals("file")
                    && uri.getAuthority() != null
                    && !uri.getAuthority().isEmpty()) {
                String supposedUri = "file:///" + uri.getAuthority() + uri.getPath();

                throw new IOException(
                        "Found local file path with authority '"
                                + uri.getAuthority()
                                + "' in path '"
                                + uri.toString()
                                + "'. Hint: Did you forget a slash? (correct path would be '"
                                + supposedUri
                                + "')");
            }

            final FSKey key = new FSKey(uri.getScheme(), uri.getAuthority());

            // See if there is a file system object in the cache
            {
                FileSystem cached = CACHE.get(key);
                if (cached != null) {
                    return cached;
                }
            }

            // this "default" initialization makes sure that the FileSystem class works
            // even when not configured with an explicit Flink configuration, like on
            // JobManager or TaskManager setup
            if (FS_FACTORIES.isEmpty()) {
                initializeWithoutPlugins(new Configuration());
            }

            // Try to create a new file system
            final FileSystem fs;
            final FileSystemFactory factory = FS_FACTORIES.get(uri.getScheme());

            if (factory != null) {
                ClassLoader classLoader = factory.getClassLoader();
                try (TemporaryClassLoaderContext ignored =
                             TemporaryClassLoaderContext.of(classLoader)) {
                    fs = factory.create(uri);
                }
            } else if (!ALLOWED_FALLBACK_FILESYSTEMS.contains(uri.getScheme())
                    && DIRECTLY_SUPPORTED_FILESYSTEM.containsKey(uri.getScheme())) {
                final Collection<String> plugins =
                        DIRECTLY_SUPPORTED_FILESYSTEM.get(uri.getScheme());
                throw new UnsupportedFileSystemSchemeException(
                        String.format(
                                "Could not find a file system implementation for scheme '%s'. The scheme is "
                                        + "directly supported by Flink through the following plugin%s: %s. Please ensure that each "
                                        + "plugin resides within its own subfolder within the plugins directory. See https://ci.apache"
                                        + ".org/projects/flink/flink-docs-stable/ops/plugins.html for more information. If you want to "
                                        + "use a Hadoop file system for that scheme, please add the scheme to the configuration fs"
                                        + ".allowed-fallback-filesystems. For a full list of supported file systems, "
                                        + "please see https://nightlies.apache.org/flink/flink-docs-stable/ops/filesystems/.",
                                uri.getScheme(),
                                plugins.size() == 1 ? "" : "s",
                                String.join(", ", plugins)));
            } else {
                try {
                    fs = FALLBACK_FACTORY.create(uri);
                } catch (UnsupportedFileSystemSchemeException e) {
                    throw new UnsupportedFileSystemSchemeException(
                            "Could not find a file system implementation for scheme '"
                                    + uri.getScheme()
                                    + "'. The scheme is not directly supported by Flink and no Hadoop file system to "
                                    + "support this scheme could be loaded. For a full list of supported file systems, "
                                    + "please see https://nightlies.apache.org/flink/flink-docs-stable/ops/filesystems/.",
                            e);
                }
            }

            CACHE.put(key, fs);
            return fs;
        } finally {
            LOCK.unlock();
        }
    }



    /**
     * Return an array containing hostnames, offset and size of portions of the given file. For a
     * nonexistent file or regions, null will be returned. This call is most helpful with DFS, where
     * it returns hostnames of machines that contain the given file. The FileSystem will simply
     * return an elt containing 'localhost'.
     */
    public abstract BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException;


    /**
     * Opens an FSDataInputStream at the indicated Path.
     *
     * @param f the file name to open
     * @param bufferSize the size of the buffer to be used.
     */
    public abstract FSDataInputStream open(Path f, int bufferSize) throws IOException;

    /**
     * Opens an FSDataInputStream at the indicated Path.
     *
     * @param f the file to open
     */
    public abstract FSDataInputStream open(Path f) throws IOException;




    /**
     * Creates a new {@link RecoverableWriter}. A recoverable writer creates streams that can
     * persist and recover their intermediate state. Persisting and recovering intermediate state is
     * a core building block for writing to files that span multiple checkpoints.
     *
     * <p>The returned object can act as a shared factory to open and recover multiple streams.
     *
     * <p>This method is optional on file systems and various file system implementations may not
     * support this method, throwing an {@code UnsupportedOperationException}.
     *
     * @return A RecoverableWriter for this file system.
     * @throws IOException Thrown, if the recoverable writer cannot be instantiated.
     */
    public RecoverableWriter createRecoverableWriter() throws IOException {
        throw new UnsupportedOperationException(
                "This file system does not support recoverable writers.");
    }




    /**
     * List the statuses of the files/directories in the given path if the path is a directory.
     *
     * @param f given path
     * @return the statuses of the files/directories in the given path
     * @throws IOException
     */
    public abstract FileStatus[] listStatus(Path f) throws IOException;


    /**
     * Check if exists.
     *
     * @param f source file
     */
    public boolean exists(final Path f) throws IOException {
        try {
            return (getFileStatus(f) != null);
        } catch (FileNotFoundException e) {
            return false;
        }
    }


    /**
     * Delete a file.
     *
     * @param f the path to delete
     * @param recursive if path is a directory and set to <code>true</code>, the directory is
     *     deleted else throws an exception. In case of a file the recursive can be set to either
     *     <code>true</code> or <code>false</code>
     * @return <code>true</code> if delete is successful, <code>false</code> otherwise
     * @throws IOException
     */
    public abstract boolean delete(Path f, boolean recursive) throws IOException;



    /**
     * Make the given file and all non-existent parents into directories. Has the semantics of Unix
     * 'mkdir -p'. Existence of the directory hierarchy is not an error.
     *
     * @param f the directory/directories to be created
     * @return <code>true</code> if at least one new directory has been created, <code>false</code>
     *     otherwise
     * @throws IOException thrown if an I/O error occurs while creating the directory
     */
    public abstract boolean mkdirs(Path f) throws IOException;


    /**
     * Opens an FSDataOutputStream at the indicated Path.
     *
     * <p>This method is deprecated, because most of its parameters are ignored by most file
     * systems. To control for example the replication factor and block size in the Hadoop
     * Distributed File system, make sure that the respective Hadoop configuration file is either
     * linked from the Flink configuration, or in the classpath of either Flink or the user code.
     *
     * @param f the file name to open
     * @param overwrite if a file with this name already exists, then if true, the file will be
     *     overwritten, and if false an error will be thrown.
     * @param bufferSize the size of the buffer to be used.
     * @param replication required block replication for the file.
     * @param blockSize the size of the file blocks
     * @throws IOException Thrown, if the stream could not be opened because of an I/O, or because a
     *     file already exists at that path and the write mode indicates to not overwrite the file.
     * @deprecated Deprecated because not well supported across types of file systems. Control the
     *     behavior of specific file systems via configurations instead.
     */
    @Deprecated
    public FSDataOutputStream create(
            Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
            throws IOException {

        return create(f, overwrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE);
    }


    /**
     * Opens an FSDataOutputStream at the indicated Path.
     *
     * @param f the file name to open
     * @param overwrite if a file with this name already exists, then if true, the file will be
     *     overwritten, and if false an error will be thrown.
     * @throws IOException Thrown, if the stream could not be opened because of an I/O, or because a
     *     file already exists at that path and the write mode indicates to not overwrite the file.
     * @deprecated Use {@link #create(Path, WriteMode)} instead.
     */
    @Deprecated
    public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
        return create(f, overwrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE);
    }


    /**
     * Opens an FSDataOutputStream to a new file at the given path.
     *
     * <p>If the file already exists, the behavior depends on the given {@code WriteMode}. If the
     * mode is set to {@link WriteMode#NO_OVERWRITE}, then this method fails with an exception.
     *
     * @param f The file path to write to
     * @param overwriteMode The action to take if a file or directory already exists at the given
     *     path.
     * @return The stream to the new file at the target path.
     * @throws IOException Thrown, if the stream could not be opened because of an I/O, or because a
     *     file already exists at that path and the write mode indicates to not overwrite the file.
     */
    public abstract FSDataOutputStream create(Path f, WriteMode overwriteMode) throws IOException;



    /**
     * Renames the file/directory src to dst.
     *
     * @param src the file/directory to rename
     * @param dst the new name of the file/directory
     * @return <code>true</code> if the renaming was successful, <code>false</code> otherwise
     * @throws IOException
     */
    public abstract boolean rename(Path src, Path dst) throws IOException;

    /**
     * Returns true if this is a distributed file system. A distributed file system here means that
     * the file system is shared among all Flink processes that participate in a cluster or job and
     * that all these processes can see the same files.
     *
     * @return True, if this is a distributed file system, false otherwise.
     */
    public abstract boolean isDistributedFS();


    /**
     * Gets a description of the characteristics of this file system.
     *
     * @deprecated this method is not used anymore.
     */
    @Deprecated
    public abstract FileSystemKind getKind();


    // ------------------------------------------------------------------------
    //  File System Methods
    // ------------------------------------------------------------------------

    /**
     * Returns the path of the file system's current working directory.
     *
     * @return the path of the file system's current working directory
     */
    public abstract Path getWorkingDirectory();


    /**
     * Returns the path of the user's home directory in this file system.
     *
     * @return the path of the user's home directory in this file system.
     */
    public abstract Path getHomeDirectory();

    /**
     * Returns a URI whose scheme and authority identify this file system.
     *
     * @return a URI whose scheme and authority identify this file system
     */
    public abstract URI getUri();

    /**
     * Return a file status object that represents the path.
     *
     * @param f The path we want information from
     * @return a FileStatus object
     * @throws FileNotFoundException when the path does not exist; IOException see specific
     *     implementation
     */
    public abstract FileStatus getFileStatus(Path f) throws IOException;



    /**
     * Loads the factories for the file systems directly supported by Flink. Aside from the {@link
     * LocalFileSystem}, these file systems are loaded via Java's service framework.
     *
     * @return A map from the file system scheme to corresponding file system factory.
     */
    private static List<FileSystemFactory> loadFileSystemFactories(
            Collection<Supplier<Iterator<FileSystemFactory>>> factoryIteratorsSuppliers) {

        final ArrayList<FileSystemFactory> list = new ArrayList<>();

        // by default, we always have the local file system factory
        list.add(new LocalFileSystemFactory());

        LOG.debug("Loading extension file systems via services");

        for (Supplier<Iterator<FileSystemFactory>> factoryIteratorsSupplier :
                factoryIteratorsSuppliers) {
            try {
                addAllFactoriesToList(factoryIteratorsSupplier.get(), list);
            } catch (Throwable t) {
                // catching Throwable here to handle various forms of class loading
                // and initialization errors
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                LOG.error("Failed to load additional file systems via services", t);
            }
        }

        return Collections.unmodifiableList(list);
    }


    private static void addAllFactoriesToList(
            Iterator<FileSystemFactory> iter, List<FileSystemFactory> list) {
        // we explicitly use an iterator here (rather than for-each) because that way
        // we can catch errors in individual service instantiations

        while (iter.hasNext()) {
            try {
                FileSystemFactory factory = iter.next();
                list.add(factory);
                LOG.debug(
                        "Added file system {}:{}",
                        factory.getScheme(),
                        factory.getClass().getSimpleName());
            } catch (Throwable t) {
                // catching Throwable here to handle various forms of class loading
                // and initialization errors
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                LOG.error("Failed to load a file system via services", t);
            }
        }
    }

    // ------------------------------------------------------------------------

    /** An identifier of a file system, via its scheme and its authority. */
    private static final class FSKey {

        /** The scheme of the file system. */
        private final String scheme;

        /** The authority of the file system. */
        @Nullable
        private final String authority;

        /**
         * Creates a file system key from a given scheme and an authority.
         *
         * @param scheme The scheme of the file system
         * @param authority The authority of the file system
         */
        public FSKey(String scheme, @Nullable String authority) {
            this.scheme = checkNotNull(scheme, "scheme");
            this.authority = authority;
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            } else if (obj != null && obj.getClass() == FSKey.class) {
                final FSKey that = (FSKey) obj;
                return this.scheme.equals(that.scheme)
                        && (this.authority == null
                        ? that.authority == null
                        : (that.authority != null
                        && this.authority.equals(that.authority)));
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return 31 * scheme.hashCode() + (authority == null ? 17 : authority.hashCode());
        }

        @Override
        public String toString() {
            return scheme + "://" + (authority != null ? authority : "");
        }
    }

}

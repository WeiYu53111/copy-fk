package flink.flink_runtime.runtime.blob;

import flink.flink_core.api.common.JobID;
import flink.flink_core.api.java.tuple.Tuple2;
import flink.flink_core.configuration.BlobServerOptions;
import flink.flink_core.configuration.Configuration;
import flink.flink_core.configuration.JobManagerOptions;
import flink.flink_core.configuration.SecurityOptions;
import flink.flink_core.util.ExceptionUtils;
import flink.flink_core.util.FileUtils;
import flink.flink_core.util.NetUtils;
import flink.flink_core.util.ShutdownHookUtil;
import flink.flink_runtime.runtime.net.SSLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ServerSocketFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static flink.flink_core.util.Preconditions.checkNotNull;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class BlobServer extends Thread implements BlobService {


    /** The log object used for debugging. */
    private static final Logger LOG = LoggerFactory.getLogger(BlobServer.class);

    /** Blob Server configuration. */
    private final Configuration blobServiceConfiguration;


    /** Blob store for distributed file storage, e.g. in HA. */
    private final BlobStore blobStore;


    /** Lock guarding concurrent file accesses. */
    private final ReadWriteLock readWriteLock;


    /** Root directory for local file storage. */
    private final File storageDir;

    /** The maximum number of concurrent connections. */
    private final int maxConnections;


    /** Time interval (ms) to run the cleanup task; also used as the default TTL. */
    private final long cleanupInterval;

    /** Timer task to execute the cleanup at regular intervals. */
    private final Timer cleanupTimer;


    /** The server socket listening for incoming connections. */
    private final ServerSocket serverSocket;


    private final ConcurrentHashMap<Tuple2<JobID, TransientBlobKey>, Long> blobExpiryTimes =
            new ConcurrentHashMap<>();


    /** Set of currently running threads. */
    private final Set<BlobServerConnection> activeConnections = new HashSet<>();

    /** Shutdown hook thread to ensure deletion of the local storage directory. */
    private final Thread shutdownHook;

    /** Counter to generate unique names for temporary files. */
    private final AtomicLong tempFileCounter = new AtomicLong(0);

    /** Indicates whether a shutdown of server component has been requested. */
    private final AtomicBoolean shutdownRequested = new AtomicBoolean();

    /**
     * Instantiates a new BLOB server and binds it to a free network port.
     *
     * @param config Configuration to be used to instantiate the BlobServer
     * @param blobStore BlobStore to store blobs persistently
     * @throws IOException thrown if the BLOB server cannot bind to a free network port or if the
     *     (local or distributed) file storage cannot be created or is not usable
     */
    public BlobServer(Configuration config, BlobStore blobStore) throws IOException {
        this.blobServiceConfiguration = checkNotNull(config);
        this.blobStore = checkNotNull(blobStore);
        this.readWriteLock = new ReentrantReadWriteLock();

        // configure and create the storage directory
        this.storageDir = BlobUtils.initLocalStorageDirectory(config);
        LOG.info("Created BLOB server storage directory {}", storageDir);

        // configure the maximum number of concurrent connections
        final int maxConnections = config.getInteger(BlobServerOptions.FETCH_CONCURRENT);
        if (maxConnections >= 1) {
            this.maxConnections = maxConnections;
        } else {
            LOG.warn(
                    "Invalid value for maximum connections in BLOB server: {}. Using default value of {}",
                    maxConnections,
                    BlobServerOptions.FETCH_CONCURRENT.defaultValue());
            this.maxConnections = BlobServerOptions.FETCH_CONCURRENT.defaultValue();
        }

        // configure the backlog of connections
        /**
         * backlog参数为socket套接字监听端口时，内核为该套接字分配的一个队列大小，
         * 在服务端还没有来得及处理请求时暂时缓存请求所用的队列。如果队列已经被客
         * 户端socket占满了，还有新的连接过来，那么ServerSocket会拒绝新的连接。
         * backlog提供了容量限制功能，避免太多的客户端socket占用太多服务器资源。
         */
        int backlog = config.getInteger(BlobServerOptions.FETCH_BACKLOG);
        if (backlog < 1) {
            LOG.warn(
                    "Invalid value for BLOB connection backlog: {}. Using default value of {}",
                    backlog,
                    BlobServerOptions.FETCH_BACKLOG.defaultValue());
            backlog = BlobServerOptions.FETCH_BACKLOG.defaultValue();
        }

        // Initializing the clean up task
        this.cleanupTimer = new Timer(true);

        this.cleanupInterval = config.getLong(BlobServerOptions.CLEANUP_INTERVAL) * 1000;
        this.cleanupTimer.schedule(
                new TransientBlobCleanupTask(
                        blobExpiryTimes, readWriteLock.writeLock(), storageDir, LOG),
                cleanupInterval,
                cleanupInterval);

        this.shutdownHook = ShutdownHookUtil.addShutdownHook(this, getClass().getSimpleName(), LOG);

        //  ----------------------- start the server -------------------

        final String serverPortRange = config.getString(BlobServerOptions.PORT);
        final Iterator<Integer> ports = NetUtils.getPortRangeFromString(serverPortRange);

        final ServerSocketFactory socketFactory;
        if (SecurityOptions.isInternalSSLEnabled(config)
                && config.getBoolean(BlobServerOptions.SSL_ENABLED)) {
            try {
                socketFactory = SSLUtils.createSSLServerSocketFactory(config);
            } catch (Exception e) {
                throw new IOException("Failed to initialize SSL for the blob server", e);
            }
        } else {
            socketFactory = ServerSocketFactory.getDefault();
        }

        final int finalBacklog = backlog;
        final String bindHost =
                config.getOptional(JobManagerOptions.BIND_HOST)
                        .orElseGet(NetUtils::getWildcardIPAddress);

        this.serverSocket =
                NetUtils.createSocketFromPorts(
                        ports,
                        (port) ->
                                socketFactory.createServerSocket(
                                        port, finalBacklog, InetAddress.getByName(bindHost)));

        if (serverSocket == null) {
            throw new IOException(
                    "Unable to open BLOB Server in specified port range: " + serverPortRange);
        }

        // start the server thread
        setName("BLOB Server listener at " + getPort());
        setDaemon(true);

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Started BLOB server at {}:{} - max concurrent requests: {} - max backlog: {}",
                    serverSocket.getInetAddress().getHostAddress(),
                    getPort(),
                    maxConnections,
                    backlog);
        }
    }

    @Override
    public PermanentBlobService getPermanentBlobService() {
        return null;
    }

    @Override
    public TransientBlobService getTransientBlobService() {
        return null;
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        cleanupTimer.cancel();

        if (shutdownRequested.compareAndSet(false, true)) {
            Exception exception = null;

            try {
                this.serverSocket.close();
            } catch (IOException ioe) {
                exception = ioe;
            }

            // wake the thread up, in case it is waiting on some operation
            interrupt();

            try {
                join();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();

                LOG.debug("Error while waiting for this thread to die.", ie);
            }

            synchronized (activeConnections) {
                if (!activeConnections.isEmpty()) {
                    for (BlobServerConnection conn : activeConnections) {
                        LOG.debug("Shutting down connection {}.", conn.getName());
                        conn.close();
                    }
                    activeConnections.clear();
                }
            }

            // Clean up the storage directory
            try {
                FileUtils.deleteDirectory(storageDir);
            } catch (IOException e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            // Remove shutdown hook to prevent resource leaks
            ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);

            if (LOG.isInfoEnabled()) {
                LOG.info(
                        "Stopped BLOB server at {}:{}",
                        serverSocket.getInetAddress().getHostAddress(),
                        getPort());
            }

            ExceptionUtils.tryRethrowIOException(exception);
        }
    }


    /** Returns the lock used to guard file accesses. */
    ReadWriteLock getReadWriteLock() {
        return readWriteLock;
    }


    @Override
    public void run() {
        try {
            while (!this.shutdownRequested.get()) {
                BlobServerConnection conn =
                        new BlobServerConnection(NetUtils.acceptWithoutTimeout(serverSocket), this);
                try {
                    synchronized (activeConnections) {
                        while (activeConnections.size() >= maxConnections) {
                            activeConnections.wait(2000);
                        }
                        activeConnections.add(conn);
                    }

                    conn.start();
                    conn = null;
                } finally {
                    if (conn != null) {
                        conn.close();
                        synchronized (activeConnections) {
                            activeConnections.remove(conn);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            if (!this.shutdownRequested.get()) {
                LOG.error("BLOB server stopped working. Shutting down", t);

                try {
                    close();
                } catch (Throwable closeThrowable) {
                    LOG.error("Could not properly close the BlobServer.", closeThrowable);
                }
            }
        }
    }



    /**
     * Returns a temporary file inside the BLOB server's incoming directory.
     *
     * @return a temporary file inside the BLOB server's incoming directory
     * @throws IOException if creating the directory fails
     */
    File createTemporaryFilename() throws IOException {
        return new File(
                BlobUtils.getIncomingDirectory(storageDir),
                String.format("temp-%08d", tempFileCounter.getAndIncrement()));
    }



    /**
     * Moves the temporary <tt>incomingFile</tt> to its permanent location where it is available for
     * use.
     *
     * @param incomingFile temporary file created during transfer
     * @param jobId ID of the job this blob belongs to or <tt>null</tt> if job-unrelated
     * @param digest BLOB content digest, i.e. hash
     * @param blobType whether this file is a permanent or transient BLOB
     * @return unique BLOB key that identifies the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while moving the file or uploading it to
     *     the HA store
     */
    BlobKey moveTempFileToStore(
            File incomingFile, @Nullable JobID jobId, byte[] digest, BlobKey.BlobType blobType)
            throws IOException {

        int retries = 10;

        int attempt = 0;
        while (true) {
            // add unique component independent of the BLOB content
            BlobKey blobKey = BlobKey.createKey(blobType, digest);
            File storageFile = BlobUtils.getStorageLocation(storageDir, jobId, blobKey);

            // try again until the key is unique (put the existence check into the lock!)
            readWriteLock.writeLock().lock();
            try {
                if (!storageFile.exists()) {
                    BlobUtils.moveTempFileToStore(
                            incomingFile,
                            jobId,
                            blobKey,
                            storageFile,
                            LOG,
                            blobKey instanceof PermanentBlobKey ? blobStore : null);
                    // add TTL for transient BLOBs:
                    if (blobKey instanceof TransientBlobKey) {
                        // must be inside read or write lock to add a TTL
                        blobExpiryTimes.put(
                                Tuple2.of(jobId, (TransientBlobKey) blobKey),
                                System.currentTimeMillis() + cleanupInterval);
                    }
                    return blobKey;
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }

            ++attempt;
            if (attempt >= retries) {
                String message =
                        "Failed to find a unique key for BLOB of job "
                                + jobId
                                + " (last tried "
                                + storageFile.getAbsolutePath()
                                + ".";
                LOG.error(message + " No retries left.");
                throw new IOException(message);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Trying to find a unique key for BLOB of job {} (retry {}, last tried {})",
                            jobId,
                            attempt,
                            storageFile.getAbsolutePath());
                }
            }
        }
    }



    /**
     * Helper to retrieve the local path of a file associated with a job and a blob key.
     *
     * <p>The blob server looks the blob key up in its local storage. If the file exists, it is
     * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
     * or a {@link FileNotFoundException} is thrown.
     *
     * <p><strong>Assumes the read lock has already been acquired.</strong>
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param blobKey blob key associated with the requested file
     * @param localFile (local) file where the blob is/should be stored
     * @throws IOException Thrown if the file retrieval failed.
     */
    void getFileInternal(@Nullable JobID jobId, BlobKey blobKey, File localFile)
            throws IOException {
        // assume readWriteLock.readLock() was already locked (cannot really check that)

        if (localFile.exists()) {
            // update TTL for transient BLOBs:
            if (blobKey instanceof TransientBlobKey) {
                // regarding concurrent operations, it is not really important which timestamp makes
                // it into the map as they are close to each other anyway, also we can simply
                // overwrite old values as long as we are in the read (or write) lock
                blobExpiryTimes.put(
                        Tuple2.of(jobId, (TransientBlobKey) blobKey),
                        System.currentTimeMillis() + cleanupInterval);
            }
            return;
        } else if (blobKey instanceof PermanentBlobKey) {
            // Try the HA blob store
            // first we have to release the read lock in order to acquire the write lock
            readWriteLock.readLock().unlock();

            // use a temporary file (thread-safe without locking)
            File incomingFile = null;
            try {
                incomingFile = createTemporaryFilename();
                blobStore.get(jobId, blobKey, incomingFile);

                readWriteLock.writeLock().lock();
                try {
                    BlobUtils.moveTempFileToStore(
                            incomingFile, jobId, blobKey, localFile, LOG, null);
                } finally {
                    readWriteLock.writeLock().unlock();
                }

                return;
            } finally {
                // delete incomingFile from a failed download
                if (incomingFile != null && !incomingFile.delete() && incomingFile.exists()) {
                    LOG.warn(
                            "Could not delete the staging file {} for blob key {} and job {}.",
                            incomingFile,
                            blobKey,
                            jobId);
                }

                // re-acquire lock so that it can be unlocked again outside
                readWriteLock.readLock().lock();
            }
        }

        throw new FileNotFoundException(
                "Local file "
                        + localFile
                        + " does not exist "
                        + "and failed to copy from blob store.");
    }


    // --------------------------------------------------------------------------------------------
    //  Path Accessors
    // --------------------------------------------------------------------------------------------

    public File getStorageDir() {
        return storageDir;
    }

    /**
     * Returns a file handle to the file associated with the given blob key on the blob server.
     *
     * <p><strong>This is only called from {@link BlobServerConnection} or unit tests.</strong>
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param key identifying the file
     * @return file handle to the file
     * @throws IOException if creating the directory fails
     */
    //@VisibleForTesting
    public File getStorageLocation(@Nullable JobID jobId, BlobKey key) throws IOException {
        return BlobUtils.getStorageLocation(storageDir, jobId, key);
    }



    void unregisterConnection(BlobServerConnection conn) {
        synchronized (activeConnections) {
            activeConnections.remove(conn);
            activeConnections.notifyAll();
        }
    }



    /**
     * Deletes the file associated with the blob key in the local storage of the blob server.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param key blob key associated with the file to be deleted
     * @return <tt>true</tt> if the given blob is successfully deleted or non-existing;
     *     <tt>false</tt> otherwise
     */
    boolean deleteInternal(@Nullable JobID jobId, TransientBlobKey key) {
        final File localFile =
                new File(
                        BlobUtils.getStorageLocationPath(storageDir.getAbsolutePath(), jobId, key));

        readWriteLock.writeLock().lock();

        try {
            if (!localFile.delete() && localFile.exists()) {
                LOG.warn(
                        "Failed to locally delete BLOB "
                                + key
                                + " at "
                                + localFile.getAbsolutePath());
                return false;
            }
            // this needs to happen inside the write lock in case of concurrent getFile() calls
            blobExpiryTimes.remove(Tuple2.of(jobId, key));
            return true;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }



}

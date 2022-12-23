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

import javax.net.ServerSocketFactory;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
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


}

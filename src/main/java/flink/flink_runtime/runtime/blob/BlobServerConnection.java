package flink.flink_runtime.runtime.blob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static flink.flink_core.util.Preconditions.checkNotNull;
import static flink.flink_runtime.runtime.blob.BlobUtils.closeSilently;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/23/2022
 */
public class BlobServerConnection extends Thread{


    /** The log object used for debugging. */
    private static final Logger LOG = LoggerFactory.getLogger(BlobServerConnection.class);


    /** The socket to communicate with the client. */
    private final Socket clientSocket;

    /** The BLOB server. */
    private final BlobServer blobServer;

    /** Read lock to synchronize file accesses. */
    private final Lock readLock;

    /**
     * Creates a new BLOB connection for a client request.
     *
     * @param clientSocket The socket to read/write data.
     * @param blobServer The BLOB server.
     */
    BlobServerConnection(Socket clientSocket, BlobServer blobServer) {
        super("BLOB connection for " + clientSocket.getRemoteSocketAddress());
        setDaemon(true);

        this.clientSocket = clientSocket;
        this.blobServer = checkNotNull(blobServer);

        ReadWriteLock readWriteLock = blobServer.getReadWriteLock();

        this.readLock = readWriteLock.readLock();
    }



    /** Closes the connection socket and lets the thread exit. */
    public void close() {
        closeSilently(clientSocket, LOG);
        interrupt();
    }
}

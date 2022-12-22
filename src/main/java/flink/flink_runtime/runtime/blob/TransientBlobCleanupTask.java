package flink.flink_runtime.runtime.blob;

import flink.flink_core.api.common.JobID;
import flink.flink_core.api.java.tuple.Tuple2;
import org.slf4j.Logger;

import java.io.File;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import static flink.flink_core.util.Preconditions.checkNotNull;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class TransientBlobCleanupTask extends TimerTask {

    /** The log object used for debugging. */
    private final Logger log;


    /** Map to store the TTL of each element stored in the local storage. */
    private ConcurrentMap<Tuple2<JobID, TransientBlobKey>, Long> blobExpiryTimes;

    /** Lock to acquire before changing file contents. */
    private Lock writeLock;

    /** Local storage directory to work on. */
    private File storageDir;
    /**
     * Creates a new cleanup timer task working with the given parameters from {@link BlobServer}
     * and {@link TransientBlobCache}.
     *
     * @param blobExpiryTimes map to store the TTL of each element stored in the local storage
     * @param writeLock lock to acquire before changing file contents
     * @param storageDir local storage directory to work on
     * @param log logger instance for debugging
     */
    TransientBlobCleanupTask(
            ConcurrentMap<Tuple2<JobID, TransientBlobKey>, Long> blobExpiryTimes,
            Lock writeLock,
            File storageDir,
            Logger log) {
        this.blobExpiryTimes = checkNotNull(blobExpiryTimes);
        this.writeLock = checkNotNull(writeLock);
        this.storageDir = checkNotNull(storageDir);
        this.log = checkNotNull(log);
    }


    @Override
    public void run() {

    }
}

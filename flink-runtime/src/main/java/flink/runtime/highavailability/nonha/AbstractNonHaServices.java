package flink.runtime.highavailability.nonha;

import flink.runtime.blob.BlobStore;
import flink.runtime.blob.VoidBlobStore;
import flink.runtime.highavailability.HighAvailabilityServices;

import java.io.IOException;

import static flink.flink_core.util.Preconditions.checkState;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/21/2022
 */
public class AbstractNonHaServices implements HighAvailabilityServices {

    private boolean shutdown;

    private final VoidBlobStore voidBlobStore;

    public AbstractNonHaServices() {
        this.voidBlobStore = new VoidBlobStore();

        shutdown = false;
    }



    protected final Object lock = new Object();
    @Override
    public BlobStore createBlobStore() throws IOException {
        synchronized (lock) {
            checkNotShutdown();

            return voidBlobStore;
        }
    }


    protected void checkNotShutdown() {
        checkState(!shutdown, "high availability services are shut down");
    }
}

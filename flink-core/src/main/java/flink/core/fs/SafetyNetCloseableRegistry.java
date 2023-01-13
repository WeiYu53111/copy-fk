package flink.core.fs;

import flink.util.AbstractCloseableRegistry;
import flink.util.Preconditions;
import flink.util.WrappingProxyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/9/2023
 */
public class SafetyNetCloseableRegistry  extends AbstractCloseableRegistry<
        WrappingProxyCloseable<? extends Closeable>,
        SafetyNetCloseableRegistry.PhantomDelegatingCloseableRef> {

    private static final Logger LOG = LoggerFactory.getLogger(SafetyNetCloseableRegistry.class);


    /** Lock for atomic modifications to reaper thread and registry count. */
    private static final Object REAPER_THREAD_LOCK = new Object();


    /** Global count of all instances of SafetyNetCloseableRegistry. */
    private static int GLOBAL_SAFETY_NET_REGISTRY_COUNT = 0;

    SafetyNetCloseableRegistry() {
        this(() -> new CloseableReaperThread());
    }


    /** Singleton reaper thread takes care of all registries in VM. */
    private static CloseableReaperThread REAPER_THREAD = null;


    //@VisibleForTesting
    SafetyNetCloseableRegistry(Supplier<CloseableReaperThread> reaperThreadSupplier) {
        super(new IdentityHashMap<>());

        synchronized (REAPER_THREAD_LOCK) {
            if (0 == GLOBAL_SAFETY_NET_REGISTRY_COUNT) {
                Preconditions.checkState(null == REAPER_THREAD);
                try {
                    REAPER_THREAD = reaperThreadSupplier.get();
                    REAPER_THREAD.start();
                } catch (Throwable throwable) {
                    REAPER_THREAD = null;
                    throw throwable;
                }
            }
            ++GLOBAL_SAFETY_NET_REGISTRY_COUNT;
        }
    }



    @Override
    protected void doRegister(
            @Nonnull WrappingProxyCloseable<? extends Closeable> wrappingProxyCloseable,
            @Nonnull Map<Closeable, PhantomDelegatingCloseableRef> closeableMap) {

        assert Thread.holdsLock(getSynchronizationLock());

        Closeable innerCloseable = WrappingProxyUtil.stripProxy(wrappingProxyCloseable);

        if (null == innerCloseable) {
            return;
        }

        PhantomDelegatingCloseableRef phantomRef =
                new PhantomDelegatingCloseableRef(
                        wrappingProxyCloseable, this, REAPER_THREAD.referenceQueue);

        closeableMap.put(innerCloseable, phantomRef);
    }

    @Override
    protected boolean doUnRegister(
            @Nonnull WrappingProxyCloseable<? extends Closeable> closeable,
            @Nonnull Map<Closeable, PhantomDelegatingCloseableRef> closeableMap) {

        assert Thread.holdsLock(getSynchronizationLock());

        Closeable innerCloseable = WrappingProxyUtil.stripProxy(closeable);

        return null != innerCloseable && closeableMap.remove(innerCloseable) != null;
    }



    /** Reaper runnable collects and closes leaking resources. */
    static class CloseableReaperThread extends Thread {

        private final ReferenceQueue<WrappingProxyCloseable<? extends Closeable>> referenceQueue;

        private volatile boolean running;

        protected CloseableReaperThread() {
            super("CloseableReaperThread");
            this.setDaemon(true);

            this.referenceQueue = new ReferenceQueue<>();
            this.running = true;
        }

        @Override
        public void run() {
            try {
                while (running) {
                    final PhantomDelegatingCloseableRef toClose =
                            (PhantomDelegatingCloseableRef) referenceQueue.remove();

                    if (toClose != null) {
                        try {
                            toClose.close();
                        } catch (Throwable t) {
                            LOG.debug("Error while closing resource via safety-net", t);
                        }
                    }
                }
            } catch (InterruptedException e) {
                // done
            }
        }

        @Override
        public void interrupt() {
            this.running = false;
            super.interrupt();
        }
    }


    /** Phantom reference to {@link WrappingProxyCloseable}. */
    static final class PhantomDelegatingCloseableRef
            extends PhantomReference<WrappingProxyCloseable<? extends Closeable>>
            implements Closeable {

        private final Closeable innerCloseable;
        private final SafetyNetCloseableRegistry closeableRegistry;
        private final String debugString;

        PhantomDelegatingCloseableRef(
                WrappingProxyCloseable<? extends Closeable> referent,
                SafetyNetCloseableRegistry closeableRegistry,
                ReferenceQueue<? super WrappingProxyCloseable<? extends Closeable>> q) {

            super(referent, q);
            this.innerCloseable =
                    Preconditions.checkNotNull(WrappingProxyUtil.stripProxy(referent));
            this.closeableRegistry = Preconditions.checkNotNull(closeableRegistry);
            this.debugString = referent.toString();
        }

        String getDebugString() {
            return debugString;
        }

        @Override
        public void close() throws IOException {
            // Mark sure the inner closeable is still registered and thus unclosed to
            // prevent duplicated and concurrent closing from registry closing. This could
            // happen if registry is closing after this phantom reference was enqueued.
            if (closeableRegistry.removeCloseableInternal(innerCloseable)) {
                LOG.warn("Closing unclosed resource via safety-net: {}", getDebugString());
                innerCloseable.close();
            }
        }
    }


}

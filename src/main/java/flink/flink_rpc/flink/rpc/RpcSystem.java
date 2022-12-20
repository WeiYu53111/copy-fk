package flink.flink_rpc.flink.rpc;


import flink.flink_core.configuration.Configuration;
import flink.flink_rpc.flink.rpc.exception.RpcLoaderException;
import flink.flink_core.util.ExceptionUtils;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/11/2022
 */
public interface RpcSystem extends RpcSystemUtils {


    static RpcSystem load(Configuration configuration) {
        final Iterator<RpcSystemLoader> iterator =
                ServiceLoader.load(RpcSystemLoader.class).iterator();

        Exception loadError = null;
        while (iterator.hasNext()) {
            final RpcSystemLoader next = iterator.next();
            try {
                return next.loadRpcSystem(configuration);
            } catch (Exception e) {
                loadError = ExceptionUtils.firstOrSuppressed(e, loadError);
            }
        }
        throw new RpcLoaderException("Could not load RpcSystem.", loadError);
    }


    RpcServiceBuilder remoteServiceBuilder(Configuration configuration, String externalAddress, String externalPortRange);

    interface RpcServiceBuilder {
        RpcServiceBuilder withComponentName(String name);

        RpcServiceBuilder withBindAddress(String bindAddress);

        RpcServiceBuilder withBindPort(int bindPort);

        RpcServiceBuilder withExecutorConfiguration(
                FixedThreadPoolExecutorConfiguration executorConfiguration);

        RpcServiceBuilder withExecutorConfiguration(
                ForkJoinExecutorConfiguration executorConfiguration);

        RpcService createAndStart() throws Exception;
    }


    class ForkJoinExecutorConfiguration {

        private final double parallelismFactor;

        private final int minParallelism;

        private final int maxParallelism;

        public ForkJoinExecutorConfiguration(
                double parallelismFactor, int minParallelism, int maxParallelism) {
            this.parallelismFactor = parallelismFactor;
            this.minParallelism = minParallelism;
            this.maxParallelism = maxParallelism;
        }

        public double getParallelismFactor() {
            return parallelismFactor;
        }

        public int getMinParallelism() {
            return minParallelism;
        }

        public int getMaxParallelism() {
            return maxParallelism;
        }
    }

    /**
     * Descriptor for creating a thread-pool with a fixed number of threads.
     */
    class FixedThreadPoolExecutorConfiguration {

        private final int minNumThreads;

        private final int maxNumThreads;

        private final int threadPriority;

        public FixedThreadPoolExecutorConfiguration(
                int minNumThreads, int maxNumThreads, int threadPriority) {
            if (threadPriority < Thread.MIN_PRIORITY || threadPriority > Thread.MAX_PRIORITY) {
                throw new IllegalArgumentException(
                        String.format(
                                "The thread priority must be within (%s, %s) but it was %s.",
                                Thread.MIN_PRIORITY, Thread.MAX_PRIORITY, threadPriority));
            }

            this.minNumThreads = minNumThreads;
            this.maxNumThreads = maxNumThreads;
            this.threadPriority = threadPriority;
        }

        public int getMinNumThreads() {
            return minNumThreads;
        }

        public int getMaxNumThreads() {
            return maxNumThreads;
        }

        public int getThreadPriority() {
            return threadPriority;
        }
    }
}

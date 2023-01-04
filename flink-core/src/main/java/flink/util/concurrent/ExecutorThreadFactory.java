package flink.util.concurrent;



import flink.util.FatalExitExceptionHandler;

import javax.annotation.Nullable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static flink.util.Preconditions.checkNotNull;


/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/20/2022
 */
public class ExecutorThreadFactory implements ThreadFactory {

    private static final String DEFAULT_POOL_NAME = "flink-executor-pool";

    private final AtomicInteger threadNumber = new AtomicInteger(1);

    private final ThreadGroup group;

    private final String namePrefix;

    private final int threadPriority;

    @Nullable private final Thread.UncaughtExceptionHandler exceptionHandler;


    @Override
    public Thread newThread(Runnable runnable) {
        Thread t = new Thread(group, runnable, namePrefix + threadNumber.getAndIncrement());
        t.setDaemon(true);

        t.setPriority(threadPriority);

        // optional handler for uncaught exceptions
        if (exceptionHandler != null) {
            t.setUncaughtExceptionHandler(exceptionHandler);
        }

        return t;
    }

    /**
     * Creates a new thread factory using the given thread pool name and the default uncaught
     * exception handler (log exception and kill process).
     *
     * @param poolName The pool name, used as the threads' name prefix
     */
    public ExecutorThreadFactory(String poolName) {
        this(poolName, FatalExitExceptionHandler.INSTANCE);
    }

    /**
     * Creates a new thread factory using the given thread pool name and the given uncaught
     * exception handler.
     *
     * @param poolName The pool name, used as the threads' name prefix
     * @param exceptionHandler The uncaught exception handler for the threads
     */
    public ExecutorThreadFactory(String poolName, Thread.UncaughtExceptionHandler exceptionHandler) {
        this(poolName, Thread.NORM_PRIORITY, exceptionHandler);
    }

    ExecutorThreadFactory(
            final String poolName,
            final int threadPriority,
            @Nullable final Thread.UncaughtExceptionHandler exceptionHandler) {
        this.namePrefix = checkNotNull(poolName, "poolName") + "-thread-";
        this.threadPriority = threadPriority;
        this.exceptionHandler = exceptionHandler;

        SecurityManager securityManager = System.getSecurityManager();
        this.group =
                (securityManager != null)
                        ? securityManager.getThreadGroup()
                        : Thread.currentThread().getThreadGroup();
    }
}

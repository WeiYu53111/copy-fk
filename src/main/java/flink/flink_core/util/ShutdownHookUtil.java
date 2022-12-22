package flink.flink_core.util;

import org.slf4j.Logger;

import static flink.flink_core.util.Preconditions.checkNotNull;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class ShutdownHookUtil {


    /** Adds a shutdown hook to the JVM and returns the Thread, which has been registered. */
    public static Thread addShutdownHook(
            final AutoCloseable service, final String serviceName, final Logger logger) {

        checkNotNull(service);
        checkNotNull(logger);

        final Thread shutdownHook =
                new Thread(
                        () -> {
                            try {
                                service.close();
                            } catch (Throwable t) {
                                logger.error(
                                        "Error during shutdown of {} via JVM shutdown hook.",
                                        serviceName,
                                        t);
                            }
                        },
                        serviceName + " shutdown hook");

        return addShutdownHookThread(shutdownHook, serviceName, logger) ? shutdownHook : null;
    }


    /**
     * Adds a shutdown hook to the JVM.
     *
     * @param shutdownHook Shutdown hook to be registered.
     * @param serviceName The name of service.
     * @param logger The logger to log.
     * @return Whether the hook has been successfully registered.
     */
    public static boolean addShutdownHookThread(
            final Thread shutdownHook, final String serviceName, final Logger logger) {

        checkNotNull(shutdownHook);
        checkNotNull(logger);

        try {
            // Add JVM shutdown hook to call shutdown of service
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            return true;
        } catch (IllegalStateException e) {
            // JVM is already shutting down. no need to do our work
        } catch (Throwable t) {
            logger.error(
                    "Cannot register shutdown hook that cleanly terminates {}.", serviceName, t);
        }
        return false;
    }

}

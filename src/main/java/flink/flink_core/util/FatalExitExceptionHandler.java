package flink.flink_core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/20/2022
 */
public class FatalExitExceptionHandler implements Thread.UncaughtExceptionHandler{

    private static final Logger LOG = LoggerFactory.getLogger(FatalExitExceptionHandler.class);
    public static final FatalExitExceptionHandler INSTANCE = new FatalExitExceptionHandler();

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        try {
            LOG.error(
                    "FATAL: Thread '{}' produced an uncaught exception. Stopping the process...",
                    t.getName(),
                    e);
        } finally {
            System.exit(-1);
            //TODO  此处没有实现FlinkSecurityManager,待补充
            /**   FlinkSecurityManager.forceProcessExit中会
             *   根据flink的参数有 可能调用Runtime.getRuntime().halt(exitCode)而不是System.exit
             *   Runtime.getRuntime().halt 不会触发 已经添加的 ShutdownHook，而 System.exit 会触发
             *
             *
             * */
            //FlinkSecurityManager.forceProcessExit(EXIT_CODE);
        }
    }
}

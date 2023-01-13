package flink.util;


import javax.annotation.Nullable;

import java.io.IOException;

import static flink.util.Preconditions.checkNotNull;


/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/11/2022
 */
public class ExceptionUtils {


    public static <T extends Throwable> T firstOrSuppressed(T newException, @Nullable T previous) {
        checkNotNull(newException, "newException");

        /**
         *        Nullable 类会找不到,maven依赖加入
         *          <dependency>
         *             <groupId>com.google.code.findbugs</groupId>
         *             <artifactId>jsr305</artifactId>
         *         </dependency>
         */
        //TODO maven parent pom中找不到  jsr305 的版本号
        if (previous == null || previous == newException) {
            return newException;
        } else {
            previous.addSuppressed(newException);
            return previous;
        }
    }


    /**
     * Tries to throw the given {@code Throwable} in scenarios where the signatures allows only
     * IOExceptions (and RuntimeException and Error). Throws this exception directly, if it is an
     * IOException, a RuntimeException, or an Error. Otherwise does nothing.
     *
     * @param t The Throwable to be thrown.
     */
    public static void tryRethrowIOException(Throwable t) throws IOException {
        if (t instanceof IOException) {
            throw (IOException) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        }
    }


    /**
     * Checks whether the given exception indicates a situation that may leave the JVM in a
     * corrupted state, meaning a state where continued normal operation can only be guaranteed via
     * clean process restart.
     *
     * <p>Currently considered fatal exceptions are Virtual Machine errors indicating that the JVM
     * is corrupted, like {@link InternalError}, {@link UnknownError}, and {@link
     * java.util.zip.ZipError} (a special case of InternalError). The {@link ThreadDeath} exception
     * is also treated as a fatal error, because when a thread is forcefully stopped, there is a
     * high chance that parts of the system are in an inconsistent state.
     *
     * @param t The exception to check.
     * @return True, if the exception is considered fatal to the JVM, false otherwise.
     */
    public static boolean isJvmFatalError(Throwable t) {
        return (t instanceof InternalError)
                || (t instanceof UnknownError)
                || (t instanceof ThreadDeath);
    }

    /**
     * Rethrows the given {@code Throwable}, if it represents an error that is fatal to the JVM or
     * an out-of-memory error. See {@link ExceptionUtils#isJvmFatalError(Throwable)} for a
     * definition of fatal errors.
     *
     * @param t The Throwable to check and rethrow.
     */
    public static void rethrowIfFatalErrorOrOOM(Throwable t) {
        if (isJvmFatalError(t) || t instanceof OutOfMemoryError) {
            throw (Error) t;
        }
    }

}

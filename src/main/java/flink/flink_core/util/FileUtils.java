package flink.flink_core.util;

import flink.flink_core.function.ThrowingConsumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;

import static flink.flink_core.util.Preconditions.checkNotNull;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/23/2022
 */
public final class FileUtils {


    /** Global lock to prevent concurrent directory deletes under Windows and MacOS. */
    private static final Object DELETE_LOCK = new Object();


    /**
     * Deletes the given directory recursively.
     *
     * <p>If the directory does not exist, this does not throw an exception, but simply does
     * nothing. It considers the fact that a directory-to-be-deleted is not present a success.
     *
     * <p>This method is safe against other concurrent deletion attempts.
     *
     * @param directory The directory to be deleted.
     * @throws IOException Thrown if the given file is not a directory, or if the directory could
     *     not be deleted for some reason, for example due to missing access/write permissions.
     */
    public static void deleteDirectory(File directory) throws IOException {
        checkNotNull(directory, "directory");

        guardIfNotThreadSafe(FileUtils::deleteDirectoryInternal, directory);
    }



    private static void deleteDirectoryInternal(File directory) throws IOException {
        if (directory.isDirectory()) {
            // directory exists and is a directory

            // empty the directory first
            try {
                cleanDirectoryInternal(directory);
            } catch (FileNotFoundException ignored) {
                // someone concurrently deleted the directory, nothing to do for us
                return;
            }

            // delete the directory. this fails if the directory is not empty, meaning
            // if new files got concurrently created. we want to fail then.
            // if someone else deleted the empty directory concurrently, we don't mind
            // the result is the same for us, after all
            Files.deleteIfExists(directory.toPath());
        } else if (directory.exists()) {
            // exists but is file, not directory
            // either an error from the caller, or concurrently a file got created
            throw new IOException(directory + " is not a directory");
        }
        // else: does not exist, which is okay (as if deleted)
    }


    private static void cleanDirectoryInternal(File directory) throws IOException {
        if (Files.isSymbolicLink(directory.toPath())) {
            // the user directories which symbolic links point to should not be cleaned.
            return;
        }
        if (directory.isDirectory()) {
            final File[] files = directory.listFiles();

            if (files == null) {
                // directory does not exist any more or no permissions
                if (directory.exists()) {
                    throw new IOException("Failed to list contents of " + directory);
                } else {
                    throw new FileNotFoundException(directory.toString());
                }
            }

            // remove all files in the directory
            for (File file : files) {
                if (file != null) {
                    deleteFileOrDirectory(file);
                }
            }
        } else if (directory.exists()) {
            throw new IOException(directory + " is not a directory but a regular file");
        } else {
            // else does not exist at all
            throw new FileNotFoundException(directory.toString());
        }
    }



    // ------------------------------------------------------------------------
    //  Deleting directories on standard File Systems
    // ------------------------------------------------------------------------

    /**
     * Removes the given file or directory recursively.
     *
     * <p>If the file or directory does not exist, this does not throw an exception, but simply does
     * nothing. It considers the fact that a file-to-be-deleted is not present a success.
     *
     * <p>This method is safe against other concurrent deletion attempts.
     *
     * @param file The file or directory to delete.
     * @throws IOException Thrown if the directory could not be cleaned for some reason, for example
     *     due to missing access/write permissions.
     */
    public static void deleteFileOrDirectory(File file) throws IOException {
        checkNotNull(file, "file");

        guardIfNotThreadSafe(FileUtils::deleteFileOrDirectoryInternal, file);
    }



    private static void guardIfNotThreadSafe(ThrowingConsumer<File, IOException> toRun, File file)
            throws IOException {
        if (OperatingSystem.isWindows()) {
            guardIfWindows(toRun, file);
            return;
        }
        if (OperatingSystem.isMac()) {
            guardIfMac(toRun, file);
            return;
        }

        toRun.accept(file);
    }



    // for Windows, we synchronize on a global lock, to prevent concurrent delete issues
    // >
    // in the future, we may want to find either a good way of working around file visibility
    // under concurrent operations (the behavior seems completely unpredictable)
    // or  make this locking more fine grained, for example  on directory path prefixes
    private static void guardIfWindows(ThrowingConsumer<File, IOException> toRun, File file)
            throws IOException {
        synchronized (DELETE_LOCK) {
            for (int attempt = 1; attempt <= 10; attempt++) {
                try {
                    toRun.accept(file);
                    break;
                } catch (AccessDeniedException e) {
                    // ah, windows...
                }

                // briefly wait and fall through the loop
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    // restore the interruption flag and error out of the method
                    Thread.currentThread().interrupt();
                    throw new IOException("operation interrupted");
                }
            }
        }
    }


    private static void deleteFileOrDirectoryInternal(File file) throws IOException {
        if (file.isDirectory()) {
            // file exists and is directory
            deleteDirectoryInternal(file);
        } else {
            // if the file is already gone (concurrently), we don't mind
            Files.deleteIfExists(file.toPath());
        }
        // else: already deleted
    }

}

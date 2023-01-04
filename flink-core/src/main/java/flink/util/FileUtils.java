package flink.util;

import flink.flink_core.function.ThrowingConsumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.util.Arrays;

import static flink.util.Preconditions.checkNotNull;


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
     * The maximum size of array to allocate for reading. See {@code MAX_BUFFER_SIZE} in {@link
     * java.nio.file.Files} for more.
     */
    private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;


    /** The size of the buffer used for reading. */
    private static final int BUFFER_SIZE = 4096;

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
            //TODO 省略MAC系统的实现
            //guardIfMac(toRun, file);
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


    /**
     * Reads all the bytes from a file. The method ensures that the file is closed when all bytes
     * have been read or an I/O error, or other runtime exception, is thrown.
     *
     * <p>This is an implementation that follow {@link
     * java.nio.file.Files#readAllBytes(java.nio.file.Path)}, and the difference is that it limits
     * the size of the direct buffer to avoid direct-buffer OutOfMemoryError. When {@link
     * java.nio.file.Files#readAllBytes(java.nio.file.Path)} or other interfaces in java API can do
     * this in the future, we should remove it.
     *
     * @param path the path to the file
     * @return a byte array containing the bytes read from the file
     * @throws IOException if an I/O error occurs reading from the stream
     * @throws OutOfMemoryError if an array of the required size cannot be allocated, for example
     *     the file is larger that {@code 2GB}
     */
    public static byte[] readAllBytes(java.nio.file.Path path) throws IOException {
        try (SeekableByteChannel channel = Files.newByteChannel(path);
             InputStream in = Channels.newInputStream(channel)) {

            long size = channel.size();
            if (size > (long) MAX_BUFFER_SIZE) {
                throw new OutOfMemoryError("Required array size too large");
            }

            return read(in, (int) size);
        }
    }

    /**
     * Reads all the bytes from an input stream. Uses {@code initialSize} as a hint about how many
     * bytes the stream will have and uses {@code directBufferSize} to limit the size of the direct
     * buffer used to read.
     *
     * @param source the input stream to read from
     * @param initialSize the initial size of the byte array to allocate
     * @return a byte array containing the bytes read from the file
     * @throws IOException if an I/O error occurs reading from the stream
     * @throws OutOfMemoryError if an array of the required size cannot be allocated
     */
    private static byte[] read(InputStream source, int initialSize) throws IOException {
        int capacity = initialSize;
        byte[] buf = new byte[capacity];
        int nread = 0;
        int n;

        for (; ; ) {
            // read to EOF which may read more or less than initialSize (eg: file
            // is truncated while we are reading)
            while ((n = source.read(buf, nread, Math.min(capacity - nread, BUFFER_SIZE))) > 0) {
                nread += n;
            }

            // if last call to source.read() returned -1, we are done
            // otherwise, try to read one more byte; if that failed we're done too
            if (n < 0 || (n = source.read()) < 0) {
                break;
            }

            // one more byte was read; need to allocate a larger buffer
            if (capacity <= MAX_BUFFER_SIZE - capacity) {
                capacity = Math.max(capacity << 1, BUFFER_SIZE);
            } else {
                if (capacity == MAX_BUFFER_SIZE) {
                    throw new OutOfMemoryError("Required array size too large");
                }
                capacity = MAX_BUFFER_SIZE;
            }
            buf = Arrays.copyOf(buf, capacity);
            buf[nread++] = (byte) n;
        }
        return (capacity == nread) ? buf : Arrays.copyOf(buf, nread);
    }

}

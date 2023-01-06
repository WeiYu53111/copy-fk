package flink.runtime.blob;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/5/2023
 */
public class BlobClientTest {


    /**
     * Validates the result of a GET operation by comparing the data from the retrieved input stream
     * to the content of the specified buffer.
     *
     * @param actualInputStream the input stream returned from the GET operation (will be closed by
     *     this method)
     * @param expectedBuf the buffer to compare the input stream's data to
     * @throws IOException thrown if an I/O error occurs while reading the input stream
     */
    static void validateGetAndClose(final InputStream actualInputStream, final byte[] expectedBuf)
            throws IOException {
        try {
            byte[] receivedBuffer = new byte[expectedBuf.length];

            int bytesReceived = 0;

            while (true) {

                final int read =
                        actualInputStream.read(
                                receivedBuffer,
                                bytesReceived,
                                receivedBuffer.length - bytesReceived);
                if (read < 0) {
                    throw new EOFException();
                }
                bytesReceived += read;

                if (bytesReceived == receivedBuffer.length) {
                    assertEquals(-1, actualInputStream.read());
                    assertArrayEquals(expectedBuf, receivedBuffer);
                    return;
                }
            }
        } finally {
            actualInputStream.close();
        }
    }

}

package flink.util;

import flink.configuration.IllegalConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.Collections;
import java.util.Iterator;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class NetUtils {

    private static final Logger LOG = LoggerFactory.getLogger(NetUtils.class);


    /** The wildcard address to listen on all interfaces (either 0.0.0.0 or ::). */
    private static final String WILDCARD_ADDRESS =
            new InetSocketAddress(0).getAddress().getHostAddress();

    /**
     * Returns an iterator over available ports defined by the range definition.
     *
     * @param rangeDefinition String describing a single port, a range of ports or multiple ranges.
     * @return Set of ports from the range definition
     * @throws NumberFormatException If an invalid string is passed.
     */
    public static Iterator<Integer> getPortRangeFromString(String rangeDefinition)
            throws NumberFormatException {
        final String[] ranges = rangeDefinition.trim().split(",");

        UnionIterator<Integer> iterators = new UnionIterator<>();

        for (String rawRange : ranges) {
            Iterator<Integer> rangeIterator;
            String range = rawRange.trim();
            int dashIdx = range.indexOf('-');
            if (dashIdx == -1) {
                // only one port in range:
                final int port = Integer.valueOf(range);
                if (!isValidHostPort(port)) {
                    throw new IllegalConfigurationException(
                            "Invalid port configuration. Port must be between 0"
                                    + "and 65535, but was "
                                    + port
                                    + ".");
                }
                rangeIterator = Collections.singleton(Integer.valueOf(range)).iterator();
            } else {
                // evaluate range
                final int start = Integer.valueOf(range.substring(0, dashIdx));
                if (!isValidHostPort(start)) {
                    throw new IllegalConfigurationException(
                            "Invalid port configuration. Port must be between 0"
                                    + "and 65535, but was "
                                    + start
                                    + ".");
                }
                final int end = Integer.valueOf(range.substring(dashIdx + 1, range.length()));
                if (!isValidHostPort(end)) {
                    throw new IllegalConfigurationException(
                            "Invalid port configuration. Port must be between 0"
                                    + "and 65535, but was "
                                    + end
                                    + ".");
                }
                rangeIterator =
                        new Iterator<Integer>() {
                            int i = start;

                            @Override
                            public boolean hasNext() {
                                return i <= end;
                            }

                            @Override
                            public Integer next() {
                                return i++;
                            }

                            @Override
                            public void remove() {
                                throw new UnsupportedOperationException("Remove not supported");
                            }
                        };
            }
            iterators.add(rangeIterator);
        }

        return iterators;
    }


    /**
     * check whether the given port is in right range when getting port from local system.
     *
     * @param port the port to check
     * @return true if the number in the range 0 to 65535
     */
    public static boolean isValidHostPort(int port) {
        return 0 <= port && port <= 65535;
    }



    /**
     * Returns the wildcard address to listen on all interfaces.
     *
     * @return Either 0.0.0.0 or :: depending on the IP setup.
     */
    public static String getWildcardIPAddress() {
        return WILDCARD_ADDRESS;
    }



    /**
     * Tries to allocate a socket from the given sets of ports.
     *
     * @param portsIterator A set of ports to choose from.
     * @param factory A factory for creating the SocketServer
     * @return null if no port was available or an allocated socket.
     */
    public static ServerSocket createSocketFromPorts(
            Iterator<Integer> portsIterator, SocketFactory factory) {
        while (portsIterator.hasNext()) {
            int port = portsIterator.next();
            LOG.debug("Trying to open socket on port {}", port);
            try {
                return factory.createSocket(port);
            } catch (IOException | IllegalArgumentException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Unable to allocate socket on port", e);
                } else {
                    LOG.info(
                            "Unable to allocate on port {}, due to error: {}",
                            port,
                            e.getMessage());
                }
            }
        }
        return null;
    }



    /** A factory for a local socket from port number. */
    @FunctionalInterface
    public interface SocketFactory {
        ServerSocket createSocket(int port) throws IOException;
    }



    /**
     * Calls {@link ServerSocket#accept()} on the provided server socket, suppressing any thrown
     * {@link SocketTimeoutException}s. This is a workaround for the underlying JDK-8237858 bug in
     * JDK 11 that can cause errant SocketTimeoutExceptions to be thrown at unexpected times.
     *
     * <p>This method expects the provided ServerSocket has no timeout set (SO_TIMEOUT of 0),
     * indicating an infinite timeout. It will suppress all SocketTimeoutExceptions, even if a
     * ServerSocket with a non-zero timeout is passed in.
     *
     * @param serverSocket a ServerSocket with {@link SocketOptions#SO_TIMEOUT SO_TIMEOUT} set to 0;
     *     if SO_TIMEOUT is greater than 0, then this method will suppress SocketTimeoutException;
     *     must not be null; SO_TIMEOUT option must be set to 0
     * @return the new Socket
     * @exception IOException see {@link ServerSocket#accept()}
     * @see <a href="https://bugs.openjdk.java.net/browse/JDK-8237858">JDK-8237858</a>
     */
    public static Socket acceptWithoutTimeout(ServerSocket serverSocket) throws IOException {
        Preconditions.checkArgument(
                serverSocket.getSoTimeout() == 0, "serverSocket SO_TIMEOUT option must be 0");
        while (true) {
            try {
                return serverSocket.accept();
            } catch (SocketTimeoutException exception) {
                // This should be impossible given that the socket timeout is set to zero
                // which indicates an infinite timeout. This is due to the underlying JDK-8237858
                // bug. We retry the accept call indefinitely to replicate the expected behavior.
            }
        }
    }

}

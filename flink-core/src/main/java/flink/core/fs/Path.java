package flink.core.fs;

import flink.core.memory.DataInputView;
import flink.core.memory.DataOutputView;
import flink.core.io.IOReadableWritable;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/4/2023
 */




/**
 * Names a file or directory in a {@link FileSystem}. Path strings use slash as the directory
 * separator. A path string is absolute if it begins with a slash.
 *
 * <p>Tailing slashes are removed from the path.
 */
public class Path implements IOReadableWritable, Serializable {


    private static final long serialVersionUID = 1L;

    /** The directory separator, a slash. */
    public static final String SEPARATOR = "/";

    /** The directory separator, a slash (character). */
    public static final char SEPARATOR_CHAR = '/';


    /** A pre-compiled regex/state-machine to match the windows drive pattern. */
    private static final Pattern WINDOWS_ROOT_DIR_REGEX = Pattern.compile("/\\p{Alpha}+:/");

    /** The internal representation of the path, a hierarchical URI. */
    private URI uri;

    /** Constructs a new (empty) path object (used to reconstruct path object after RPC call). */
    public Path() {}

    /**
     * Constructs a path object from a given URI.
     *
     * @param uri the URI to construct the path object from
     */
    public Path(URI uri) {
        this.uri = uri;
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public Path(String parent, String child) {
        this(new Path(parent), new Path(child));
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public Path(Path parent, String child) {
        this(parent, new Path(child));
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public Path(String parent, Path child) {
        this(new Path(parent), child);
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public Path(Path parent, Path child) {
        // Add a slash to parent's path so resolution is compatible with URI's
        URI parentUri = parent.uri;
        final String parentPath = parentUri.getPath();
        if (!(parentPath.equals("/") || parentPath.equals(""))) {
            try {
                parentUri =
                        new URI(
                                parentUri.getScheme(),
                                parentUri.getAuthority(),
                                parentUri.getPath() + "/",
                                null,
                                null);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }

        if (child.uri.getPath().startsWith(Path.SEPARATOR)) {
            child =
                    new Path(
                            child.uri.getScheme(),
                            child.uri.getAuthority(),
                            child.uri.getPath().substring(1));
        }

        final URI resolved = parentUri.resolve(child.uri);
        initialize(resolved.getScheme(), resolved.getAuthority(), resolved.getPath());
    }


    /**
     * Construct a path from a String. Path strings are URIs, but with unescaped elements and some
     * additional normalization.
     *
     * @param pathString the string to construct a path from
     */
    public Path(String pathString) {
        pathString = checkPathArg(pathString);

        // We can't use 'new URI(String)' directly, since it assumes things are
        // escaped, which we don't require of Paths.

        // add a slash in front of paths with Windows drive letters
        if (hasWindowsDrive(pathString, false)) {
            pathString = "/" + pathString;
        }

        // parse uri components
        String scheme = null;
        String authority = null;

        int start = 0;

        // parse uri scheme, if any
        final int colon = pathString.indexOf(':');
        final int slash = pathString.indexOf('/');
        if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a
            // scheme
            scheme = pathString.substring(0, colon);
            start = colon + 1;
        }

        // parse uri authority, if any
        if (pathString.startsWith("//", start)
                && (pathString.length() - start > 2)) { // has authority
            final int nextSlash = pathString.indexOf('/', start + 2);
            final int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
            authority = pathString.substring(start + 2, authEnd);
            start = authEnd;
        }

        // uri path is the rest of the string -- query & fragment not supported
        final String path = pathString.substring(start, pathString.length());

        initialize(scheme, authority, path);
    }



    /**
     * Checks if the provided path string is either null or has zero length and throws a {@link
     * IllegalArgumentException} if any of the two conditions apply.
     *
     * @param path the path string to be checked
     * @return The checked path.
     */
    private String checkPathArg(String path) {
        // disallow construction of a Path from an empty string
        if (path == null) {
            throw new IllegalArgumentException("Can not create a Path from a null string");
        }
        if (path.length() == 0) {
            throw new IllegalArgumentException("Can not create a Path from an empty string");
        }
        return path;
    }

    /**
     * Initializes a path object given the scheme, authority and path string.
     *
     * @param scheme the scheme string.
     * @param authority the authority string.
     * @param path the path string.
     */
    private void initialize(String scheme, String authority, String path) {
        try {
            this.uri = new URI(scheme, authority, normalizePath(path), null, null).normalize();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Construct a Path from a scheme, an authority and a path string.
     *
     * @param scheme the scheme string
     * @param authority the authority string
     * @param path the path string
     */
    public Path(String scheme, String authority, String path) {
        path = checkPathArg(path);
        initialize(scheme, authority, path);
    }



    @Override
    public void write(DataOutputView out) throws IOException {

    }

    @Override
    public void read(DataInputView in) throws IOException {

    }



    /**
     * Checks if the provided path string contains a windows drive letter.
     *
     * @param path the path to check
     * @param slashed true to indicate the first character of the string is a slash, false otherwise
     * @return <code>true</code> if the path string contains a windows drive letter, false otherwise
     */
    private boolean hasWindowsDrive(String path, boolean slashed) {
        final int start = slashed ? 1 : 0;
        return path.length() >= start + 2
                && (!slashed || path.charAt(0) == '/')
                && path.charAt(start + 1) == ':'
                && ((path.charAt(start) >= 'A' && path.charAt(start) <= 'Z')
                || (path.charAt(start) >= 'a' && path.charAt(start) <= 'z'));
    }



    /**
     * Normalizes a path string.
     *
     * @param path the path string to normalize
     * @return the normalized path string
     */
    private String normalizePath(String path) {
        // remove consecutive slashes & backslashes
        path = path.replace("\\", "/");
        path = path.replaceAll("/+", "/");

        // remove tailing separator
        if (path.endsWith(SEPARATOR)
                && !path.equals(SEPARATOR)
                && // UNIX root path
                !WINDOWS_ROOT_DIR_REGEX.matcher(path).matches()) { // Windows root path)

            // remove tailing slash
            path = path.substring(0, path.length() - SEPARATOR.length());
        }

        return path;
    }



    /**
     * Returns the FileSystem that owns this Path.
     *
     * @return the FileSystem that owns this Path
     * @throws IOException thrown if the file system could not be retrieved
     */
    public FileSystem getFileSystem() throws IOException {
        return FileSystem.get(this.toUri());
    }



    /**
     * Converts the path object to a {@link URI}.
     *
     * @return the {@link URI} object converted from the path object
     */
    public URI toUri() {
        return uri;
    }

}

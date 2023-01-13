package flink.core.io;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/13/2023
 */

import java.io.IOException;

/**
 * A simple serializer interface for versioned serialization.
 *
 * <p>The serializer has a version (returned by {@link #getVersion()}) which can be attached to the
 * serialized data. When the serializer evolves, the version can be used to identify with which
 * prior version the data was serialized.
 *
 * <pre>{@code
 * MyType someObject = ...;
 * SimpleVersionedSerializer<MyType> serializer = ...;
 *
 * byte[] serializedData = serializer.serialize(someObject);
 * int version = serializer.getVersion();
 *
 * MyType deserialized = serializer.deserialize(version, serializedData);
 *
 * byte[] someOldData = ...;
 * int oldVersion = ...;
 * MyType deserializedOldObject = serializer.deserialize(oldVersion, someOldData);
 *
 * }</pre>
 *
 * @param <E> The data type serialized / deserialized by this serializer.
 */
//@Internal
public interface SimpleVersionedSerializer<E> extends Versioned {

    /**
     * Gets the version with which this serializer serializes.
     *
     * @return The version of the serialization schema.
     */
    @Override
    int getVersion();

    /**
     * Serializes the given object. The serialization is assumed to correspond to the current
     * serialization version (as returned by {@link #getVersion()}.
     *
     * @param obj The object to serialize.
     * @return The serialized data (bytes).
     * @throws IOException Thrown, if the serialization fails.
     */
    byte[] serialize(E obj) throws IOException;

    /**
     * De-serializes the given data (bytes) which was serialized with the scheme of the indicated
     * version.
     *
     * @param version The version in which the data was serialized
     * @param serialized The serialized data
     * @return The deserialized object
     * @throws IOException Thrown, if the deserialization fails.
     */
    E deserialize(int version, byte[] serialized) throws IOException;
}

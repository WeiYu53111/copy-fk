package flink.core.io;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 1/4/2023
 */


import flink.core.memory.DataInputView;
import flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * This interface must be implemented by every class whose objects have to be serialized to their
 * binary representation and vice-versa. In particular, records have to implement this interface in
 * order to specify how their data can be transferred to a binary representation.
 *
 * <p>When implementing this Interface make sure that the implementing class has a default
 * (zero-argument) constructor!
 */
public interface IOReadableWritable {


    /**
     * Writes the object's internal data to the given data output view.
     *
     * @param out the output view to receive the data.
     * @throws IOException thrown if any error occurs while writing to the output stream
     */
    void write(DataOutputView out) throws IOException;

    /**
     * Reads the object's internal data from the given data input view.
     *
     * @param in the input view to read the data from
     * @throws IOException thrown if any error occurs while reading from the input stream
     */
    void read(DataInputView in) throws IOException;

}

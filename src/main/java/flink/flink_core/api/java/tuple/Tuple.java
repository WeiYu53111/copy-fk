package flink.flink_core.api.java.tuple;

import flink.flink_core.types.NullFieldException;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/21/2022
 */
public abstract class Tuple implements  java.io.Serializable {


    private static final long serialVersionUID = 1L;

    public static final int MAX_ARITY = 25;

    /**
     * Gets the field at the specified position.
     *
     * @param pos The position of the field, zero indexed.
     * @return The field at the specified position.
     * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger
     *     than the number of fields.
     */
    public abstract <T> T getField(int pos);

    /**
     * Gets the field at the specified position, throws NullFieldException if the field is null.
     * Used for comparing key fields.
     *
     * @param pos The position of the field, zero indexed.
     * @return The field at the specified position.
     * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger
     *     than the number of fields.
     * @throws NullFieldException Thrown, if the field at pos is null.
     */
    public <T> T getFieldNotNull(int pos) {
        T field = getField(pos);
        if (field != null) {
            return field;
        } else {
            throw new NullFieldException(pos);
        }
    }

    /**
     * Sets the field at the specified position.
     *
     * @param value The value to be assigned to the field at the specified position.
     * @param pos The position of the field, zero indexed.
     * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger
     *     than the number of fields.
     */
    public abstract <T> void setField(T value, int pos);

    /**
     * Gets the number of field in the tuple (the tuple arity).
     *
     * @return The number of fields in the tuple.
     */
    public abstract int getArity();

    /**
     * Shallow tuple copy.
     *
     * @return A new Tuple with the same fields as this.
     */
    public abstract <T extends Tuple> T copy();

}

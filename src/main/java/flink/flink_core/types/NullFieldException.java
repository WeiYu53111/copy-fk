package flink.flink_core.types;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/21/2022
 */
public class NullFieldException extends RuntimeException {
    /** UID for serialization interoperability. */
    private static final long serialVersionUID = -8820467525772321173L;

    private final int fieldPos;

    /**
     * Constructs an {@code NullFieldException} with a default message, referring to given field
     * number as the null field.
     *
     * @param fieldIdx The index of the field that was null, but expected to hold a value.
     */
    public NullFieldException(int fieldIdx) {
        super("Field " + fieldIdx + " is null, but expected to hold a value.");
        this.fieldPos = fieldIdx;
    }

}

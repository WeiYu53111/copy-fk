package flink.flink_core.configuration.description;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/22/2022
 */
public class TextElement implements BlockElement, InlineElement{

    private final String format;
    private final List<InlineElement> elements;
    private final EnumSet<TextStyle> textStyles = EnumSet.noneOf(TextStyle.class);

    @Override
    public void format(Formatter formatter) {
        formatter.format(this);
    }


    public String getFormat() {
        return format;
    }

    public static TextElement text(String format, InlineElement... elements) {
        return new TextElement(format, Arrays.asList(elements));
    }

    private TextElement(String format, List<InlineElement> elements) {
        this.format = format;
        this.elements = elements;
    }

    public List<InlineElement> getElements() {
        return elements;
    }


    /** Styles that can be applied to {@link TextElement} e.g. code, bold etc. */
    public enum TextStyle {
        CODE
    }

    public EnumSet<TextStyle> getStyles() {
        return textStyles;
    }



    /**
     * Creates a simple block of text.
     *
     * @param text a simple block of text
     * @return block of text
     */
    public static TextElement text(String text) {
        return new TextElement(text, Collections.emptyList());
    }


    /**
     * Creates a block of text formatted as code.
     *
     * @param text a block of text that will be formatted as code
     * @return block of text formatted as code
     */
    public static TextElement code(String text) {
        TextElement element = text(text);
        element.textStyles.add(TextStyle.CODE);
        return element;
    }


}

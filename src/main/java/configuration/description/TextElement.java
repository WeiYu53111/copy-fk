package configuration.description;

import java.util.Arrays;
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

    @Override
    public void format(Formatter formatter) {
        formatter.format(this);
    }

    public static TextElement text(String format, InlineElement... elements) {
        return new TextElement(format, Arrays.asList(elements));
    }

    private TextElement(String format, List<InlineElement> elements) {
        this.format = format;
        this.elements = elements;
    }

}

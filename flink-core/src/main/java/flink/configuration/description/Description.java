package flink.configuration.description;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description   提供丰富的文本格式化展示
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 11/22/2022
 */
public class Description {

    private final List<BlockElement> blocks;

    public List<BlockElement> getBlocks() {
        return blocks;
    }

    public static DescriptionBuilder builder() {
        return new DescriptionBuilder();
    }

    public static class DescriptionBuilder {

        private final List<BlockElement> blocks = new ArrayList<>();

        public DescriptionBuilder text(String format, InlineElement... elements) {
            blocks.add(TextElement.text(format, elements));
            return this;
        }

        public Description build() {
            return new Description(blocks);
        }

    }

    private Description(List<BlockElement> blocks) {
        this.blocks = blocks;
    }

}

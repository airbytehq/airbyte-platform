import { StoryObj } from "@storybook/react";

import { Text } from "components/ui/Text";

import { Box } from "./Box";
import styles from "./Box.stories.module.scss";

export default {
  title: "ui/Box",
  component: Box,
} as StoryObj<typeof Box>;

export const Default: StoryObj<typeof Box> = {
  args: {
    className: styles.box,
  },
  render: (args) => (
    <>
      <Text size="lg">
        The dashed border appears outside the <code>{"<Box>"}</code> to help visualize margins.
      </Text>
      <div style={{ border: "1px dashed black" }}>
        <Box {...args}>
          <Text size="lg">
            This text is wrapped in a <code>{"<Box>"}</code>. It has standard spacing values for margin and padding,
            which you can control via props. You can use this component to achieve simple layouts without needing to
            write a custom CSS module. The gray background is only added in storybook to help visualize the spacing.
          </Text>
        </Box>
      </div>
    </>
  ),
};

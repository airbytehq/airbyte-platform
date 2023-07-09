import { StoryObj } from "@storybook/react";

import { Text } from "components/ui/Text";

import { Box } from "./Box";
import styles from "./Box.stories.module.scss";

export default {
  title: "ui/Box",
  component: Box,
  argTypes: {
    className: {
      control: false,
    },
  },
} as StoryObj<typeof Box>;

export const Default: StoryObj<typeof Box> = {
  args: {
    className: styles.box,
  },
  render: (args) => (
    <>
      <Text size="lg">
        The dashed border is around the <code>{"<Box>"}</code>. You'll see margins around the `Box` tomato colored.
      </Text>
      <div style={{ display: "flex", backgroundColor: "tomato" }}>
        <Box {...args} className={styles.box}>
          <Text size="lg">
            This text is wrapped in a <code>{"<Box>"}</code>. It has standard spacing values for margin and padding,
            which you can control via props. You can use this component to achieve simple layouts without needing to
            write a custom CSS module. The gray background is only added in storybook to help visualize the spacing.
          </Text>
        </Box>
      </div>
      <Text size="lg">
        Just some text below the <code>Box</code>.
      </Text>
    </>
  ),
};

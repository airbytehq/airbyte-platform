import { Meta, StoryFn } from "@storybook/react";

import { Pre } from "./Pre";

export default {
  title: "Ui/Pre",
  component: Pre,
} as Meta<typeof Pre>;

const Template: StoryFn<typeof Pre> = ({ children }) => <Pre>{children}</Pre>;

export const Primary = Template.bind({});
Primary.args = {
  children: `{
  key1: "value1",
  key2: "value2",
}`,
};

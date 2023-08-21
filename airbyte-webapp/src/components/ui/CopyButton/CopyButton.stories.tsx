import { Meta, StoryFn } from "@storybook/react";

import { CopyButton } from "./CopyButton";

export default {
  title: "Ui/CopyButton",
  component: CopyButton,
} as Meta<typeof CopyButton>;

const Template: StoryFn<typeof CopyButton> = (args) => <CopyButton {...args} />;

export const Default = Template.bind({});
Default.args = {
  content: "The content to copy",
};

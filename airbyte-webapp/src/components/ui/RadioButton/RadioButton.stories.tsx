import { Meta, StoryFn } from "@storybook/react";

import { RadioButton } from "./RadioButton";

export default {
  title: "Ui/RadioButton",
  component: RadioButton,
  argTypes: {
    disabled: { control: "boolean" },
    checked: { control: "boolean" },
  },
} as Meta<typeof RadioButton>;

const Template: StoryFn<typeof RadioButton> = (args) => <RadioButton {...args} />;

export const Default = Template.bind({});
Default.args = {};

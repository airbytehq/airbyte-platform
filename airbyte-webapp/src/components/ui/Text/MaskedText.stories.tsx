import { Meta, StoryFn } from "@storybook/react";

import { MaskedText } from "./MaskedText";

export default {
  title: "Ui/MaskedText",
  component: MaskedText,
} as Meta<typeof MaskedText>;

const Template: StoryFn<typeof MaskedText> = (args) => <MaskedText {...args} />;

export const Default = Template.bind({});
Default.args = {
  children: "MyVerySecretString",
};

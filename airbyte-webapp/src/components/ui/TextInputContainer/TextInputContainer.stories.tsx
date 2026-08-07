import { StoryFn, Meta } from "@storybook/react";

import { TextInputContainer } from "./TextInputContainer";

export default {
  title: "Ui/TextInputContainer",
  component: TextInputContainer,
} as Meta<typeof TextInputContainer>;

const Template: StoryFn<typeof TextInputContainer> = (args) => <TextInputContainer {...args} />;

export const WithInput = Template.bind({});
WithInput.args = {
  children: <input type="text" placeholder="With text..." />,
};

export const WithTextArea = Template.bind({});
WithTextArea.args = {
  children: <textarea placeholder="With textarea..." />,
};

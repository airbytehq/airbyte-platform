import { StoryFn, Meta } from "@storybook/react";

import { TextWithHTML } from "./TextWithHTML";

export default {
  title: "UI/TextWithHTML",
  component: TextWithHTML,
  argTypes: {},
} as Meta<typeof TextWithHTML>;

const Template: StoryFn<typeof TextWithHTML> = (args) => <TextWithHTML {...args} />;

const text = `
  <h2>List Items</h2>
  <ul>
    <li>List item 1</li>
    <li>List item 2</li>
  </ul>
`;

export const Primary = Template.bind({});
Primary.args = {
  text,
};

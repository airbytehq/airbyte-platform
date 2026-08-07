import { StoryFn, Meta } from "@storybook/react";

import { Overlay } from "./Overlay";

export default {
  title: "UI/Overlay",
  component: Overlay,
  argTypes: {},
} as Meta<typeof Overlay>;

const Template: StoryFn<typeof Overlay> = (args) => <Overlay {...args} />;

export const Primary = Template.bind({});
Primary.args = {};

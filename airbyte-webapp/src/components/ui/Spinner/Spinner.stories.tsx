import { StoryFn, Meta } from "@storybook/react";

import { Spinner } from "./Spinner";

export default {
  title: "UI/Spinner",
  component: Spinner,
  argTypes: {
    small: { type: "boolean", required: false },
  },
} as Meta<typeof Spinner>;

const Template: StoryFn<typeof Spinner> = (args) => <Spinner {...args} />;

export const Primary = Template.bind({});

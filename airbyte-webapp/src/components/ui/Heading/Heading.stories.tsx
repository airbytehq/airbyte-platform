import { StoryObj } from "@storybook/react";

import { Heading } from "./Heading";

export default {
  title: "Ui/Heading",
  component: Heading,
} as StoryObj<typeof Heading>;

export const Primary: StoryObj<typeof Heading> = {
  args: {
    as: "h1",
    size: "md",
    children:
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
  },
};

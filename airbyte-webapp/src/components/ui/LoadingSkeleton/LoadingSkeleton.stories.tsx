import { StoryObj } from "@storybook/react";

import { LoadingSkeleton } from "components/ui/LoadingSkeleton";

import { FlexContainer } from "../Flex";

export default {
  title: "ui/LoadingSkeleton",
  component: LoadingSkeleton,
  argTypes: {},
} as StoryObj<typeof LoadingSkeleton>;

export const Default: StoryObj<typeof LoadingSkeleton> = {
  args: {
    variant: "shimmer",
  },
  argTypes: {
    variant: {
      control: { type: "radio" },
      options: ["shimmer", "magic"],
    },
  },
  render: ({ variant }) => (
    <FlexContainer direction="column" gap="xl">
      <LoadingSkeleton variant={variant} />
      <LoadingSkeleton variant={variant} />
      <LoadingSkeleton variant={variant} />
    </FlexContainer>
  ),
};

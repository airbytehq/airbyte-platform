import { StoryObj } from "@storybook/react";

import { LoadingSkeleton } from "components/ui/LoadingSkeleton";

import { FlexContainer } from "../Flex";

export default {
  title: "ui/LoadingSkeleton",
  component: LoadingSkeleton,
  argTypes: {},
} as StoryObj<typeof LoadingSkeleton>;

export const Default: StoryObj<typeof LoadingSkeleton> = {
  args: {},
  render: () => (
    <FlexContainer direction="column" gap="xl">
      <LoadingSkeleton />
      <LoadingSkeleton />
      <LoadingSkeleton />
    </FlexContainer>
  ),
};

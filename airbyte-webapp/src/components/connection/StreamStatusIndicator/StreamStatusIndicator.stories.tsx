import { Meta, StoryObj } from "@storybook/react";

import { StreamStatusIndicator } from "./StreamStatusIndicator";
import { StreamStatusType } from "../StreamStatus/streamStatusUtils";

export default {
  title: "connection/StreamStatusIndicator",
  component: StreamStatusIndicator,
} as Meta<typeof StreamStatusIndicator>;

type Story = StoryObj<typeof StreamStatusIndicator>;

export const Primary: Story = {
  args: {
    status: StreamStatusType.UpToDate,
  },
};

import { Meta, StoryObj } from "@storybook/react";

import { StreamStatusIndicator } from "./StreamStatusIndicator";
import { ConnectionStatusIndicatorStatus } from "../ConnectionStatusIndicator";

export default {
  title: "connection/StreamStatusIndicator",
  component: StreamStatusIndicator,
} as Meta<typeof StreamStatusIndicator>;

type Story = StoryObj<typeof StreamStatusIndicator>;

export const Primary: Story = {
  args: {
    status: ConnectionStatusIndicatorStatus.OnTime,
  },
};

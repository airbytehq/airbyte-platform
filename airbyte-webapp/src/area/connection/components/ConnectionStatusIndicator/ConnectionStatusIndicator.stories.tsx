import { Meta, StoryObj } from "@storybook/react";

import { ConnectionSyncStatus } from "core/api/types/AirbyteClient";

import { ConnectionStatusIndicator } from "./ConnectionStatusIndicator";

export default {
  title: "connection/ConnectionStatusIndicator",
  component: ConnectionStatusIndicator,
} as Meta<typeof ConnectionStatusIndicator>;

type Story = StoryObj<typeof ConnectionStatusIndicator>;

export const Primary: Story = {
  args: {
    status: ConnectionSyncStatus.synced,
  },
};

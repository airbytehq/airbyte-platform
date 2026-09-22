import { Meta, StoryObj } from "@storybook/react";

import { Badge } from "./Badge";

export default {
  title: "UI/Badge",
  component: Badge,
} as Meta<typeof Badge>;

export const Default: StoryObj<typeof Badge> = {
  args: {
    children: "Some fancy badge",
  },
};

export const Teal: StoryObj<typeof Badge> = {
  args: {
    variant: "teal",
    radius: "2xs",
    uppercase: false,
    children: "SCIM Enabled",
  },
};

export const Purple: StoryObj<typeof Badge> = {
  args: {
    variant: "purple",
    radius: "2xs",
    uppercase: false,
    children: "Data replication",
  },
};

export const Coral: StoryObj<typeof Badge> = {
  args: {
    variant: "coral",
    radius: "2xs",
    uppercase: false,
    children: "Agent",
  },
};

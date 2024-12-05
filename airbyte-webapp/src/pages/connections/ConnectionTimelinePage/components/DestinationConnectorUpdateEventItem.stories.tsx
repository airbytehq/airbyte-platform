import { StoryObj } from "@storybook/react";

import { ConnectorIds } from "area/connector/utils";
import { ConnectionEventType } from "core/api/types/AirbyteClient";

import { DestinationConnectorUpdateEventItem } from "./DestinationConnectorUpdateEventItem";

export default {
  title: "connection-timeline/DestinationConnectorUpdateEventItem",
  component: DestinationConnectorUpdateEventItem,
} as StoryObj<typeof DestinationConnectorUpdateEventItem>;

const baseEvent = {
  id: "fc62442f-1cc1-4a57-a385-11ca7507c649",
  connectionId: "a90ab3d6-b5cb-4e43-8c97-5a4ab8f2f7d9",
  eventType: ConnectionEventType.CONNECTOR_UPDATE,
  summary: {
    name: "End-to-End Testing (/dev/null)",
    destinationDefinitionId: ConnectorIds.Destinations.EndToEndTesting,
  },
  user: {
    id: "00000000-0000-0000-0000-000000000000",
    email: "volodymyr.s.petrov@globallogic.com",
    name: "Volodymyr Petrov",
  },
  createdAt: 1732114841,
};

export const UpgradedVersionByUser: StoryObj<typeof DestinationConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        newDockerImageTag: "2.1.0",
        oldDockerImageTag: "2.0.5",
        changeReason: "USER",
      },
    },
  },
};

export const UpgradedVersionBySystem: StoryObj<typeof DestinationConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        newDockerImageTag: "2.1.0",
        oldDockerImageTag: "2.0.5",
        changeReason: "SYSTEM",
      },
    },
  },
};

export const DowngradedVersionByUser: StoryObj<typeof DestinationConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        newDockerImageTag: "1.0.1",
        oldDockerImageTag: "3.0.0",
        changeReason: "USER",
      },
    },
  },
};

export const DowngradedVersionBySystem: StoryObj<typeof DestinationConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        newDockerImageTag: "1.0.1",
        oldDockerImageTag: "3.0.0",
        changeReason: "SYSTEM",
      },
    },
  },
};

export const UpdatedVersion: StoryObj<typeof DestinationConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        newDockerImageTag: "4.0",
        oldDockerImageTag: "3.5.6-rc1",
        changeReason: "USER",
      },
    },
  },
};

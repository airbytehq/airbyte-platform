import { StoryObj } from "@storybook/react";

import { ConnectionEventType } from "core/api/types/AirbyteClient";

import { ConnectorUpdateEventItem } from "./ConnectorUpdateEventItem";

export default {
  title: "connection-timeline/ConnectorUpdateEventItem",
  component: ConnectorUpdateEventItem,
} as StoryObj<typeof ConnectorUpdateEventItem>;

const baseEvent = {
  id: "fc62442f-1cc1-4a57-a385-11ca7507c649",
  connectionId: "a90ab3d6-b5cb-4e43-8c97-5a4ab8f2f7d9",
  eventType: ConnectionEventType.CONNECTOR_UPDATE,
  summary: {
    connectorName: "End-to-End Testing (/dev/null)",
    triggeredBy: undefined,
  },
  user: {
    id: "00000000-0000-0000-0000-000000000000",
    email: "volodymyr.s.petrov@globallogic.com",
    name: "Volodymyr Petrov",
    isDeleted: false,
  },
  createdAt: 1732114841,
};

export const SourceUpgradedVersionByUser: StoryObj<typeof ConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        connectorType: "SOURCE",
        toVersion: "2.1.0",
        fromVersion: "2.0.5",
        changeReason: "USER",
      },
    },
  },
};

export const SourceUpgradedVersionBySystem: StoryObj<typeof ConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        connectorType: "SOURCE",
        toVersion: "2.1.0",
        fromVersion: "2.0.5",
        changeReason: "SYSTEM",
      },
    },
  },
};

export const SourceDowngradedVersionByUser: StoryObj<typeof ConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        connectorType: "SOURCE",
        toVersion: "1.0.1",
        fromVersion: "3.0.0",
        changeReason: "USER",
      },
    },
  },
};

export const SourceDowngradedVersionBySystem: StoryObj<typeof ConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        connectorType: "SOURCE",
        toVersion: "1.0.1",
        fromVersion: "3.0.0",
        changeReason: "SYSTEM",
      },
    },
  },
};

export const SourceUpdatedVersion: StoryObj<typeof ConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        connectorType: "SOURCE",
        toVersion: "4.0",
        fromVersion: "3.5.6-rc1",
        changeReason: "USER",
      },
    },
  },
};

export const DestinationUpgradedVersionByUser: StoryObj<typeof ConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        connectorType: "DESTINATION",
        toVersion: "2.1.0",
        fromVersion: "2.0.5",
        changeReason: "USER",
      },
    },
  },
};

export const DestinationUpgradedVersionBySystem: StoryObj<typeof ConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        connectorType: "DESTINATION",
        toVersion: "2.1.0",
        fromVersion: "2.0.5",
        changeReason: "SYSTEM",
      },
    },
  },
};

export const DestinationDowngradedVersionByUser: StoryObj<typeof ConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        connectorType: "DESTINATION",
        toVersion: "1.0.1",
        fromVersion: "3.0.0",
        changeReason: "USER",
      },
    },
  },
};

export const DestinationDowngradedVersionBySystem: StoryObj<typeof ConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        connectorType: "DESTINATION",
        toVersion: "1.0.1",
        fromVersion: "3.0.0",
        changeReason: "SYSTEM",
      },
    },
  },
};

export const DestinationUpdatedVersion: StoryObj<typeof ConnectorUpdateEventItem> = {
  args: {
    event: {
      ...baseEvent,
      summary: {
        ...baseEvent.summary,
        connectorType: "DESTINATION",
        toVersion: "4.0",
        fromVersion: "3.5.6-rc1",
        changeReason: "USER",
      },
    },
  },
};

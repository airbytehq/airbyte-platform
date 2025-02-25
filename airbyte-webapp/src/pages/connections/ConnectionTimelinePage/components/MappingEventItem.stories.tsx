import { StoryObj } from "@storybook/react";

import { MappingEventItem } from "./MappingEventItem";

export default {
  title: "connection-timeline/MappingEventItem",
  component: MappingEventItem,
} as StoryObj<typeof MappingEventItem>;

const baseEvent = {
  id: "fc62442f-1cc1-4a57-a385-11ca7507c649",
  connectionId: "a90ab3d6-b5cb-4e43-8c97-5a4ab8f2f7d9",
  user: {
    id: "00000000-0000-0000-0000-000000000000",
    email: "volodymyr.s.petrov@globallogic.com",
    name: "Volodymyr Petrov",
    isDeleted: false,
  },
  createdAt: 1732114841,
};

export const CreateMapping: StoryObj<typeof MappingEventItem> = {
  args: {
    event: {
      ...baseEvent,
      eventType: "MAPPING_CREATE",
      summary: {
        streamName: "users",
        streamNamespace: undefined,
        mapperType: "field-renaming",
      },
    },
  },
};

export const CreateMappingWithNamespace: StoryObj<typeof MappingEventItem> = {
  args: {
    event: {
      ...baseEvent,
      eventType: "MAPPING_CREATE",
      summary: {
        streamName: "users",
        streamNamespace: "public",
        mapperType: "field-renaming",
      },
    },
  },
};

export const UpdateMapping: StoryObj<typeof MappingEventItem> = {
  args: {
    event: {
      ...baseEvent,
      eventType: "MAPPING_UPDATE",
      summary: {
        streamName: "pokemon",
        streamNamespace: undefined,
        mapperType: "hashing",
      },
    },
  },
};

export const DeleteMapping: StoryObj<typeof MappingEventItem> = {
  args: {
    event: {
      ...baseEvent,
      eventType: "MAPPING_DELETE",
      summary: {
        streamName: "pokemon",
        streamNamespace: undefined,
        mapperType: "encryption",
      },
    },
  },
};

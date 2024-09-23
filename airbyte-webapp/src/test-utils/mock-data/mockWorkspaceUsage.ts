import dayjs from "dayjs";

import { mockSource, mockDestination } from "test-utils";
import { mockConnection } from "test-utils/mock-data/mockConnection";
import { mockDestinationDefinition } from "test-utils/mock-data/mockDestination";
import { mockSourceDefinition } from "test-utils/mock-data/mockSource";

import { WorkspaceUsageRead } from "core/api/types/AirbyteClient";

const firstConnectionRead = { ...mockConnection, breakingChange: false, name: "First connection" };
const firstSource = { ...mockSource, name: "Source 1" };

const secondConnectionRead = { ...firstConnectionRead, name: "Second connection" };
const secondSource = { ...firstSource, sourceId: "source-id-2", name: "Source 2" };

const todayStart = dayjs().startOf("day").toISOString();
const todayEnd = dayjs().endOf("day").toISOString();

const oneDayAgoStart = dayjs().subtract(1, "day").startOf("day").toISOString();
const oneDayAgoEnd = dayjs().subtract(1, "day").endOf("day").toISOString();

const twoDaysAgoStart = dayjs().subtract(2, "day").startOf("day").toISOString();
const twoDaysAgoEnd = dayjs().subtract(2, "day").endOf("day").toISOString();

export const mockWorkspaceUsage: WorkspaceUsageRead = {
  data: [
    {
      connection: firstConnectionRead,
      destination: mockDestination,
      destinationDefinition: mockDestinationDefinition,
      source: firstSource,
      sourceDefinition: mockSourceDefinition,
      usage: {
        free: [
          {
            quantity: 10.0,
            timeframeEnd: oneDayAgoEnd,
            timeframeStart: oneDayAgoStart,
          },
        ],
        internal: [
          {
            quantity: 10.0,
            timeframeEnd: oneDayAgoEnd,
            timeframeStart: oneDayAgoStart,
          },
        ],
        regular: [
          {
            quantity: 25.0,
            timeframeEnd: oneDayAgoEnd,
            timeframeStart: oneDayAgoStart,
          },
          {
            quantity: 25.0,
            timeframeEnd: oneDayAgoEnd,
            timeframeStart: oneDayAgoStart,
          },
          {
            quantity: 3.5,
            timeframeEnd: twoDaysAgoEnd,
            timeframeStart: twoDaysAgoStart,
          },
        ],
      },
    },
    {
      connection: secondConnectionRead,
      destination: mockDestination,
      destinationDefinition: mockDestinationDefinition,
      source: secondSource,
      sourceDefinition: mockSourceDefinition,
      usage: {
        free: [
          {
            quantity: 10.0,
            timeframeEnd: todayEnd,
            timeframeStart: todayStart,
          },
        ],
        internal: [
          {
            quantity: 100.0,
            timeframeEnd: todayEnd,
            timeframeStart: todayStart,
          },
          {
            quantity: 40.0,
            timeframeEnd: dayjs("2024-07-24").endOf("day").toISOString(),
            timeframeStart: dayjs("2024-07-24").startOf("day").toISOString(),
          },
        ],
        regular: [
          {
            quantity: 10.0,
            timeframeEnd: todayEnd,
            timeframeStart: todayStart,
          },
          {
            quantity: 10.0,
            timeframeEnd: dayjs("2024-07-27").endOf("day").toISOString(),
            timeframeStart: dayjs("2024-07-27").startOf("day").toISOString(),
          },
        ],
      },
    },
  ],
};

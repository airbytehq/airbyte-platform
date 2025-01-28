import { mockConnection } from "test-utils/mock-data/mockConnection";
import { mockJob } from "test-utils/mock-data/mockJob";
import { mockUser } from "test-utils/mock-data/mockUser";

import { ConnectionEvent, ConnectionEventType } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";

import { validateAndMapEvent } from "./ConnectionTimelineAllEventsList";

jest.mock("core/utils/datadog", () => ({
  trackError: jest.fn(),
}));

describe("#validateAndMapEvent", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });
  const validTimelineEvents: ConnectionEvent[] = [
    {
      id: "1",
      createdAt: 1728936010,
      eventType: ConnectionEventType.CONNECTION_ENABLED,
      connectionId: mockConnection.connectionId,
      summary: {},
      user: {
        id: mockUser.userId,
        email: mockUser.email,
        name: mockUser.name,
        isDeleted: false,
      },
    },
    {
      id: "2",
      createdAt: 1728936015,
      eventType: ConnectionEventType.CONNECTION_DISABLED,
      connectionId: mockConnection.connectionId,
      summary: {
        disabledReason: "SCHEMA_CHANGES_ARE_BREAKING",
      },
    },
    {
      id: "2",
      createdAt: 1728936015,
      eventType: ConnectionEventType.CONNECTION_DISABLED,
      connectionId: mockConnection.connectionId,
      summary: {
        disabledReason: "INVALID_PAYMENT_METHOD",
      },
    },
    {
      id: "2",
      createdAt: 1728936015,
      eventType: ConnectionEventType.CONNECTION_DISABLED,
      connectionId: mockConnection.connectionId,
      summary: {
        disabledReason: "INVOICE_MARKED_UNCOLLECTIBLE",
      },
    },
    {
      id: "3",
      createdAt: 1728936020,
      eventType: ConnectionEventType.SYNC_STARTED,
      connectionId: mockConnection.connectionId,
      user: {
        id: mockUser.userId,
        email: mockUser.email,
        name: mockUser.name,
        isDeleted: false,
      },
      summary: {
        jobId: mockJob.id,
        startTimeEpochSeconds: 1728936020,
      },
    },
    {
      id: "4",
      createdAt: 1728936040,
      eventType: ConnectionEventType.SYNC_CANCELLED,
      connectionId: mockConnection.connectionId,
      user: {
        id: mockUser.userId,
        email: mockUser.email,
        name: mockUser.name,
        isDeleted: false,
      },
      summary: {
        jobId: mockJob.id,
        startTimeEpochSeconds: 1728936020,
        endTimeEpochSeconds: 172893604,
        attemptsCount: 1,
        bytesLoaded: 123123142,
        recordsLoaded: 123123,
      },
    },
  ];
  it("lists components for timeline items", () => {
    const validatedEvents = validTimelineEvents
      .map((event) => validateAndMapEvent(event))
      .filter((event) => event !== null);
    expect(validatedEvents).toHaveLength(validTimelineEvents.length);
  });
  it("removes invalid events from list and triggers error tracking", () => {
    const eventsWithInvalidEvent = [
      ...validTimelineEvents,
      {
        id: "5",
        createdAt: 1728936100,
        eventType: ConnectionEventType.CONNECTION_SETTINGS_UPDATE,
        connectionId: mockConnection.connectionId,
        summary: {
          patches: {
            test: {
              to: "test",
              from: "test",
            },
          },
        },
        user: {
          id: mockUser.userId,
          email: mockUser.email,
          name: mockUser.name,
          isDeleted: false,
        },
      },
    ];
    const validatedEvents = eventsWithInvalidEvent
      .map((event) => validateAndMapEvent(event))
      .filter((event) => event !== null);
    expect(validatedEvents).toHaveLength(validTimelineEvents.length);
    expect(trackError).toHaveBeenCalledTimes(1);
  });
  it("removes events with only resourceRequirement patches but does not trigger error handling", () => {
    const eventsWithResourceUpdate = [
      ...validTimelineEvents,
      {
        id: "5",
        createdAt: 1728936130,
        eventType: ConnectionEventType.CONNECTION_SETTINGS_UPDATE,
        connectionId: mockConnection.connectionId,
        summary: {
          patches: {
            resourceRequirements: {
              to: "test",
              from: "test",
            },
          },
        },
        user: {
          id: mockUser.userId,
          email: mockUser.email,
          name: mockUser.name,
          isDeleted: false,
        },
      },
    ];

    const validatedEvents = eventsWithResourceUpdate
      .map((event) => validateAndMapEvent(event))
      .filter((event) => event !== null);
    expect(validatedEvents).toHaveLength(validTimelineEvents.length);
    expect(trackError).toHaveBeenCalledTimes(0);
  });
});

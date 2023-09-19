import dayjs from "dayjs";

import { ConsumptionRead } from "core/api/types/CloudApi";

export const mockWorkspaceUsageConnection: ConsumptionRead["connection"] = {
  connectionId: "connection1",
  connectionName: "my connection",
  status: "active",
  creditsConsumed: undefined,
  destinationConnectionName: "my destination",
  destinationDefinitionId: "destinationA",
  destinationDefinitionName: "Test Destination",
  destinationIcon: "",
  destinationId: "destinationA",
  sourceConnectionName: "my source",
  sourceDefinitionId: "def",
  sourceDefinitionName: "Test Source",
  sourceIcon: "",
  sourceId: "source1",
  connectionScheduleType: "manual",
  connectionScheduleTimeUnit: undefined,
  connectionScheduleUnits: undefined,
  destinationReleaseStage: "alpha",
  sourceReleaseStage: "alpha",
  sourceSupportLevel: "community",
  destinationSupportLevel: "community",
  sourceCustom: false,
  destinationCustom: false,
};

export const secondMockWorkspaceConnection: ConsumptionRead["connection"] = {
  ...mockWorkspaceUsageConnection,
  connectionId: "connection2",
  connectionName: "my second connection",
  sourceId: "source2",
};

export const thirdMockWorkspaceConnection: ConsumptionRead["connection"] = {
  ...mockWorkspaceUsageConnection,
  connectionId: "connection3",
  connectionName: "my third connection",
  sourceId: "source3",
  destinationDefinitionId: "destinationB",
  destinationId: "destinationB",
};

const NOW = dayjs();

export const mockConsumptionThirtyDay: ConsumptionRead[] = [
  {
    connection: mockWorkspaceUsageConnection,
    freeUsage: 100,
    billedCost: 200,
    startTime: NOW.subtract(1, "day").format("YYYY-MM-DD"),
    endTime: NOW.format("YYYY-MM-DD"),
  },
  {
    connection: mockWorkspaceUsageConnection,
    freeUsage: 300,
    billedCost: 400,
    startTime: NOW.format("YYYY-MM-DD"),
    endTime: NOW.add(1, "day").format("YYYY-MM-DD"),
  },
  {
    connection: mockWorkspaceUsageConnection,
    freeUsage: 1,
    billedCost: 1,
    startTime: NOW.format("YYYY-MM-DD"),
    endTime: NOW.add(1, "day").format("YYYY-MM-DD"),
  },
  {
    connection: secondMockWorkspaceConnection,
    freeUsage: 10,
    billedCost: 20,
    startTime: NOW.subtract(1, "day").format("YYYY-MM-DD"),
    endTime: NOW.format("YYYY-MM-DD"),
  },
  {
    connection: secondMockWorkspaceConnection,
    freeUsage: 10,
    billedCost: 20,
    startTime: NOW.format("YYYY-MM-DD"),
    endTime: NOW.add(1, "day").format("YYYY-MM-DD"),
  },
  {
    connection: thirdMockWorkspaceConnection,
    freeUsage: 5,
    billedCost: 5,
    startTime: NOW.format("YYYY-MM-DD"),
    endTime: NOW.add(1, "day").format("YYYY-MM-DD"),
  },
  {
    connection: thirdMockWorkspaceConnection,
    freeUsage: 7,
    billedCost: 7,
    startTime: NOW.subtract(1, "day").format("YYYY-MM-DD"),
    endTime: NOW.format("YYYY-MM-DD"),
  },
];

export const mockConsumptionSixMonths: ConsumptionRead[] = [
  // last week -- idx 26
  {
    connection: mockWorkspaceUsageConnection,
    freeUsage: 100,
    billedCost: 200,
    startTime: NOW.startOf("week").subtract(1, "week").format("YYYY-MM-DD"),
    endTime: NOW.endOf("week").subtract(1, "week").format("YYYY-MM-DD"),
  },
  {
    connection: mockWorkspaceUsageConnection,
    freeUsage: 300,
    billedCost: 400,
    startTime: NOW.startOf("week").subtract(1, "week").format("YYYY-MM-DD"),
    endTime: NOW.endOf("week").subtract(1, "week").format("YYYY-MM-DD"),
  },
  {
    connection: mockWorkspaceUsageConnection,
    freeUsage: 1,
    billedCost: 1,
    startTime: NOW.startOf("week").subtract(1, "week").format("YYYY-MM-DD"),
    endTime: NOW.endOf("week").subtract(1, "week").format("YYYY-MM-DD"),
  },
  // three weeks ago -- idx 23
  {
    connection: secondMockWorkspaceConnection,
    freeUsage: 10,
    billedCost: 20,
    startTime: NOW.startOf("week").subtract(3, "week").format("YYYY-MM-DD"),
    endTime: NOW.endOf("week").subtract(3, "week").format("YYYY-MM-DD"),
  },
  {
    connection: secondMockWorkspaceConnection,
    freeUsage: 10,
    billedCost: 20,
    startTime: NOW.startOf("week").subtract(3, "week").format("YYYY-MM-DD"),
    endTime: NOW.endOf("week").subtract(3, "week").format("YYYY-MM-DD"),
  },
  {
    connection: thirdMockWorkspaceConnection,
    freeUsage: 5,
    billedCost: 5,
    startTime: NOW.startOf("week").subtract(3, "week").format("YYYY-MM-DD"),
    endTime: NOW.endOf("week").subtract(3, "week").format("YYYY-MM-DD"),
  },
  {
    connection: thirdMockWorkspaceConnection,
    freeUsage: 7,
    billedCost: 7,
    startTime: NOW.startOf("week").subtract(3, "week").format("YYYY-MM-DD"),
    endTime: NOW.endOf("week").subtract(3, "week").format("YYYY-MM-DD"),
  },
];

export const mockConsumptionYear: ConsumptionRead[] = [
  // this month
  {
    connection: mockWorkspaceUsageConnection,
    freeUsage: 100,
    billedCost: 200,
    startTime: NOW.startOf("month").format("YYYY-MM-DD"),
    endTime: NOW.endOf("month").format("YYYY-MM-DD"),
  },
  {
    connection: mockWorkspaceUsageConnection,
    freeUsage: 300,
    billedCost: 400,
    startTime: NOW.startOf("month").format("YYYY-MM-DD"),
    endTime: NOW.endOf("month").format("YYYY-MM-DD"),
  },
  {
    connection: mockWorkspaceUsageConnection,
    freeUsage: 1,
    billedCost: 1,
    startTime: NOW.startOf("month").format("YYYY-MM-DD"),
    endTime: NOW.endOf("month").format("YYYY-MM-DD"),
  },
  // three weeks ago -- idx 23
  {
    connection: secondMockWorkspaceConnection,
    freeUsage: 10,
    billedCost: 20,
    startTime: NOW.startOf("month").subtract(4, "month").format("YYYY-MM-DD"),
    endTime: NOW.endOf("month").subtract(4, "month").format("YYYY-MM-DD"),
  },
  {
    connection: secondMockWorkspaceConnection,
    freeUsage: 10,
    billedCost: 20,
    startTime: NOW.startOf("month").subtract(4, "month").format("YYYY-MM-DD"),
    endTime: NOW.endOf("month").subtract(4, "month").format("YYYY-MM-DD"),
  },
  {
    connection: thirdMockWorkspaceConnection,
    freeUsage: 5,
    billedCost: 5,
    startTime: NOW.startOf("month").subtract(4, "month").format("YYYY-MM-DD"),
    endTime: NOW.endOf("month").subtract(4, "month").format("YYYY-MM-DD"),
  },
  {
    connection: thirdMockWorkspaceConnection,
    freeUsage: 7,
    billedCost: 7,
    startTime: NOW.startOf("month").subtract(4, "month").format("YYYY-MM-DD"),
    endTime: NOW.endOf("month").subtract(4, "month").format("YYYY-MM-DD"),
  },
];

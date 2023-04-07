import dayjs from "dayjs";

import { WorkspaceRead } from "core/request/AirbyteClient";
import { ConsumptionPerConnectionPerTimeframe } from "packages/cloud/lib/domain/cloudWorkspaces/types";

export const mockWorkspace: WorkspaceRead = {
  workspaceId: "47c74b9b-9b89-4af1-8331-4865af6c4e4d",
  customerId: "55dd55e2-33ac-44dc-8d65-5aa7c8624f72",
  email: "krishna@airbyte.com",
  name: "47c74b9b-9b89-4af1-8331-4865af6c4e4d",
  slug: "47c74b9b-9b89-4af1-8331-4865af6c4e4d",
  initialSetupComplete: true,
  displaySetupWizard: false,
  anonymousDataCollection: false,
  news: false,
  securityUpdates: false,
  notifications: [],
};

export const mockWorkspaceUsageConnection: ConsumptionPerConnectionPerTimeframe["connection"] = {
  connectionId: "connection1",
  connectionName: "my connection",
  status: "active",
  creditsConsumed: null,
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
  connectionScheduleTimeUnit: null,
  connectionScheduleUnits: null,
};

export const secondMockWorkspaceConnection: ConsumptionPerConnectionPerTimeframe["connection"] = {
  ...mockWorkspaceUsageConnection,
  connectionId: "connection2",
  connectionName: "my second connection",
  sourceId: "source2",
};

export const thirdMockWorkspaceConnection: ConsumptionPerConnectionPerTimeframe["connection"] = {
  ...mockWorkspaceUsageConnection,
  connectionId: "connection3",
  connectionName: "my third connection",
  sourceId: "source3",
  destinationDefinitionId: "destinationB",
  destinationId: "destinationB",
};

export const mockConsumptionData = [
  {
    connection: mockWorkspaceUsageConnection,
    timeframe: dayjs().subtract(1, "day").format("YYYY-MM-DD"),
    freeUsage: 100,
    billedCost: 200,
  },

  {
    connection: mockWorkspaceUsageConnection,
    timeframe: dayjs().format("YYYY-MM-DD"),
    freeUsage: 300,
    billedCost: 400,
  },
  {
    connection: mockWorkspaceUsageConnection,
    timeframe: dayjs().format("YYYY-MM-DD"),
    freeUsage: 1,
    billedCost: 1,
  },
  {
    connection: secondMockWorkspaceConnection,
    timeframe: dayjs().subtract(1, "day").format("YYYY-MM-DD"),
    freeUsage: 10,
    billedCost: 20,
  },
  {
    connection: secondMockWorkspaceConnection,
    timeframe: dayjs().format("YYYY-MM-DD"),
    freeUsage: 10,
    billedCost: 20,
  },
  {
    connection: thirdMockWorkspaceConnection,
    timeframe: dayjs().format("YYYY-MM-DD"),
    freeUsage: 5,
    billedCost: 5,
  },
  {
    connection: thirdMockWorkspaceConnection,
    timeframe: dayjs().subtract(1, "day").format("YYYY-MM-DD"),
    freeUsage: 7,
    billedCost: 7,
  },
];

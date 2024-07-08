import {
  ActorDefinitionVersionBreakingChanges,
  ActorDefinitionVersionRead,
  ConnectionScheduleData,
  ConnectionScheduleType,
  SchemaChange,
  SupportState,
  WebBackendConnectionListItem,
} from "../../core/api/types/AirbyteClient";

interface EntityTableDataItem {
  entityId: string;
  entityName: string;
  connectorName: string;
  connectEntities: Array<{
    name: string;
    connector: string;
    status: string;
    lastSyncStatus: string | null;
  }>;
  enabled: boolean;
  lastSync?: number | null;
  connectorIcon?: string;
  isActive: boolean;
  breakingChanges?: ActorDefinitionVersionBreakingChanges;
  isVersionOverrideApplied: boolean;
  supportState?: SupportState;
}

interface ConnectionTableDataItem {
  connectionId: string;
  name: string;
  entityName: string;
  connectorName: string;
  enabled: boolean;
  isSyncing?: boolean;
  status?: string;
  lastSync?: number | null;
  scheduleData?: ConnectionScheduleData;
  scheduleType?: ConnectionScheduleType;
  schemaChange: SchemaChange;
  source: ActorDefinitionVersionRead;
  destination: ActorDefinitionVersionRead;
  lastSyncStatus: string | null;
  connectorIcon?: string;
  entityIcon?: string;
  connection: WebBackendConnectionListItem;
}

const enum Status {
  ACTIVE = "active",
  INACTIVE = "inactive",
  FAILED = "failed",
  CANCELLED = "cancelled",
  EMPTY = "empty",
  PENDING = "pending",
}

enum SortOrderEnum {
  DESC = "desc",
  ASC = "asc",
}

export type { ConnectionTableDataItem, EntityTableDataItem };
export { Status, SortOrderEnum };

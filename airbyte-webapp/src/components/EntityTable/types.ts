import {
  ActorDefinitionVersionBreakingChanges,
  ActorDefinitionVersionRead,
  ConnectionScheduleData,
  ConnectionScheduleType,
  SchemaChange,
  SupportState,
  Tag,
  WebBackendConnectionListItem,
} from "../../core/api/types/AirbyteClient";

interface EntityTableDataItem {
  entityId: string;
  entityName: string;
  connectorName: string;
  enabled: boolean;
  connectorIcon?: string;
  isActive: boolean;
  breakingChanges?: ActorDefinitionVersionBreakingChanges;
  isVersionOverrideApplied: boolean;
  supportState?: SupportState;
  numConnections?: number;
  lastSync?: number;
  connectionJobStatuses?: Record<string, number>;
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
  tags?: Tag[];
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

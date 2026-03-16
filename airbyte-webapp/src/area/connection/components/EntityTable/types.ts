import {
  ActorDefinitionVersionRead,
  ConnectionScheduleData,
  ConnectionScheduleType,
  SchemaChange,
  Tag,
  WebBackendConnectionListItem,
} from "core/api/types/AirbyteClient";

/**
 * Extended WebBackendConnectionListItem with onDemandEnabled field.
 * TODO: Remove this extension once backend adds onDemandEnabled to the API response.
 * See: https://github.com/airbytehq/hydra-issues-internal/issues/113
 */
export interface WebBackendConnectionListItemWithOnDemand extends WebBackendConnectionListItem {
  onDemandEnabled?: boolean;
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
  connection: WebBackendConnectionListItemWithOnDemand;
  tags?: Tag[];
}

const enum Status {
  ACTIVE = "active",
  INACTIVE = "inactive",
  FAILED = "failed",
  CANCELLED = "cancelled",
  EMPTY = "empty",
  PENDING = "pending",
  QUEUED = "queued",
}

enum SortOrderEnum {
  DESC = "desc",
  ASC = "asc",
}

export type { ConnectionTableDataItem };
export { Status, SortOrderEnum };

import type { DestinationHandle, SourceHandle } from "./connectors";
import type { Handle } from "./handle";
import type { AirbyteCatalog, WebBackendConnectionRead } from "@src/core/api/types/AirbyteClient";

import { createHandle } from "./handle";

/** Fields the store derives: IDs, the source and destination objects, actor definition versions, and catalogId. */
type DerivedConnectionField =
  | "connectionId"
  | "source"
  | "sourceId"
  | "destination"
  | "destinationId"
  | "syncCatalog"
  | "catalogId"
  | "sourceActorDefinitionVersion"
  | "destinationActorDefinitionVersion";

/** Any other WebBackendConnectionRead field can be set. Unset fields get the defaults in store.ts. */
export type ConnectionInput = {
  source: SourceHandle;
  destination: DestinationHandle;
  catalog: AirbyteCatalog;
} & Partial<Omit<WebBackendConnectionRead, DerivedConnectionField>>;

export type ConnectionHandle = Handle<"connection", ConnectionInput>;

export function connection(input: ConnectionInput): ConnectionHandle {
  return createHandle("connection", input);
}

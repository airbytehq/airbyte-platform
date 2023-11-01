import { AirbyteCatalog, AirbyteStreamAndConfiguration } from "../../request/AirbyteClient";

/**
 * @deprecated will be removed during clean up - https://github.com/airbytehq/airbyte-platform-internal/issues/8639
 */
export interface SyncSchemaStream extends AirbyteStreamAndConfiguration {
  /**
   * This field is not returned from API and is used to track unique objects
   */
  id?: string;
}

/**
 * @deprecated will be removed during clean up - https://github.com/airbytehq/airbyte-platform-internal/issues/8639
 */
export interface SyncSchema extends AirbyteCatalog {
  streams: SyncSchemaStream[];
}

export type Path = string[];

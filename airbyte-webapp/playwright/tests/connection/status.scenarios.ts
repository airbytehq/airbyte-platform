import type { MockAirbyteScenario } from "../../helpers/mockApiState";

import { fakerExistingConnectionScenario } from "../../helpers/mockApiState";

export const manualSyncScenario: MockAirbyteScenario = fakerExistingConnectionScenario;

export const pendingConnectionScenario: MockAirbyteScenario = {
  ...fakerExistingConnectionScenario,
  connections: fakerExistingConnectionScenario.connections.map((connection) => ({
    ...connection,
    syncCatalog: {
      streams: connection.syncCatalog.streams.map((stream) => ({
        ...stream,
        config: { ...stream.config!, selected: false },
      })),
    },
  })),
};

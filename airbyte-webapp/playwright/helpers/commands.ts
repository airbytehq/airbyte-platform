import { APIRequestContext } from "@playwright/test";
import { GetCommandStatus200, GetDiscoverCommandOutput200 } from "@src/core/api/types/AirbyteClient";
import { v4 as uuidv4 } from "uuid";

import { getApiBaseUrl } from "./api";

export const commandsAPI = {
  discoverSchema: async (request: APIRequestContext, actorId: string) => {
    const commandId = uuidv4();
    await request.post(`${getApiBaseUrl()}/commands/run/discover`, {
      data: {
        id: commandId,
        actor_id: actorId,
      },
    });
    let commandRunning = true;
    while (commandRunning) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      const response = await request.post(`${getApiBaseUrl()}/commands/status`, { data: { id: commandId } });
      const { status }: GetCommandStatus200 = await response.json();
      if (status === "completed" || status === "cancelled") {
        commandRunning = false;
      }
    }
    const commandOutputResponse = await request.post(`${getApiBaseUrl()}/commands/output/discover`, {
      data: { id: commandId },
    });
    const output: GetDiscoverCommandOutput200 = await commandOutputResponse.json();
    return output;
  },
};

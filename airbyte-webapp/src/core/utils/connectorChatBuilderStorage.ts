import { z } from "zod";

export const CONNECTOR_CHAT_ACTIONS = {
  LAUNCH_BUILDER: "launch-connector-builder",
  SET_UP_NEW_CONNECTOR: "set-up-new-connector",
} as const;

const setUpNewConnectorSchema = z.object({
  definitionId: z.string(),
  connectorType: z.union([z.literal("source"), z.literal("destination")]),
});

const launchBuilderSchema = z.object({
  name: z.string(),
  stream: z.string(),
  documentation_url: z.string().url(),
  submit_form: z.union([z.string(), z.boolean()]).optional(),
});

export type SetUpNewConnectorParams = z.infer<typeof setUpNewConnectorSchema>;
export type LaunchBuilderParams = z.infer<typeof launchBuilderSchema>;

type ConnectorChatAction = (typeof CONNECTOR_CHAT_ACTIONS)[keyof typeof CONNECTOR_CHAT_ACTIONS];

const getSetUpNewConnectorParams = (queryParams: Array<[string, string]>) => {
  const paramNames = ["definition_id", "connector_type"];
  const connectorChatBuilderInfo = Object.fromEntries(queryParams.filter(([key]) => paramNames.includes(key)));

  if (!connectorChatBuilderInfo.definition_id || !connectorChatBuilderInfo.connector_type) {
    return;
  }

  sessionStorage.setItem(
    CONNECTOR_CHAT_ACTIONS.SET_UP_NEW_CONNECTOR,
    JSON.stringify({
      definitionId: connectorChatBuilderInfo.definition_id,
      connectorType: connectorChatBuilderInfo.connector_type,
    })
  );
};

const getLaunchBuilderParams = (queryParams: Array<[string, string]>) => {
  const paramNames = [
    "connector_builder_name",
    "connector_builder_stream",
    "connector_builder_documentation_url",
    "connector_builder_submit_form",
  ];
  const connectorChatBuilderInfo = Object.fromEntries(queryParams.filter(([key]) => paramNames.includes(key)));
  const documentation_url = decodeURIComponent(connectorChatBuilderInfo.connector_builder_documentation_url);

  if (
    !connectorChatBuilderInfo.connector_builder_name ||
    !connectorChatBuilderInfo.connector_builder_stream ||
    !documentation_url
  ) {
    return;
  }

  sessionStorage.setItem(
    CONNECTOR_CHAT_ACTIONS.LAUNCH_BUILDER,
    JSON.stringify({
      name: connectorChatBuilderInfo.connector_builder_name,
      stream: connectorChatBuilderInfo.connector_builder_stream,
      documentation_url,
      submit_form: connectorChatBuilderInfo.connector_builder_submit_form,
    })
  );
};
export const storeConnectorChatBuilderFromQuery = (queryString?: string): void => {
  if (!queryString) {
    return;
  }

  const queryParams = Array.from(new URLSearchParams(queryString).entries());
  const connectorChatAction = queryParams.find(([key]) => key === "connector_chat_action");

  if (!connectorChatAction) {
    return;
  }

  if (connectorChatAction[1] === CONNECTOR_CHAT_ACTIONS.SET_UP_NEW_CONNECTOR) {
    getSetUpNewConnectorParams(queryParams);
  }

  if (connectorChatAction[1] === CONNECTOR_CHAT_ACTIONS.LAUNCH_BUILDER) {
    getLaunchBuilderParams(queryParams);
  }
};

export const getSetUpNewConnectorParamsFromStorage = (): SetUpNewConnectorParams | undefined => {
  const storedParams = sessionStorage.getItem(CONNECTOR_CHAT_ACTIONS.SET_UP_NEW_CONNECTOR);
  if (!storedParams) {
    return undefined;
  }

  try {
    const parsedParams = JSON.parse(storedParams);

    return setUpNewConnectorSchema.parse(parsedParams);
  } catch (error) {
    console.error("Invalid stored params for action:", CONNECTOR_CHAT_ACTIONS.SET_UP_NEW_CONNECTOR, error);
    return undefined;
  }
};

export const getLaunchBuilderParamsFromStorage = (): LaunchBuilderParams | undefined => {
  const storedParams = sessionStorage.getItem(CONNECTOR_CHAT_ACTIONS.LAUNCH_BUILDER);
  if (!storedParams) {
    return undefined;
  }

  try {
    const parsedParams = JSON.parse(storedParams);

    const params = launchBuilderSchema.parse(parsedParams);
    if (typeof params.submit_form === "string") {
      params.submit_form = params.submit_form === "true";
    }
    return params;
  } catch (error) {
    console.error("Invalid stored params for action:", CONNECTOR_CHAT_ACTIONS.LAUNCH_BUILDER, error);
    return undefined;
  }
};

export const clearConnectorChatBuilderStorage = (action: ConnectorChatAction): void => {
  sessionStorage.removeItem(action);
};

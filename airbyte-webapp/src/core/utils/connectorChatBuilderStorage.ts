const CONNECTOR_CHAT_BUILDER_STORAGE_KEY = "launch-connector-builder";

export const storeConnectorChatBuilderFromQuery = (queryString?: string): void => {
  if (queryString) {
    const queryParams = Array.from(new URLSearchParams(queryString).entries());

    const paramNames = [
      "connector_builder_name",
      "connector_builder_stream",
      "connector_builder_documentation_url",
      "connector_builder_submit_form",
    ];
    const connectorChatBuilderInfo = Object.fromEntries(queryParams.filter(([key]) => paramNames.includes(key)));
    console.log("connectorChatBuilderInfo", connectorChatBuilderInfo);

    const documentation_url = decodeURIComponent(connectorChatBuilderInfo.connector_builder_documentation_url);

    if (
      connectorChatBuilderInfo.connector_builder_name &&
      connectorChatBuilderInfo.connector_builder_stream &&
      documentation_url
    ) {
      sessionStorage.setItem(
        CONNECTOR_CHAT_BUILDER_STORAGE_KEY,
        JSON.stringify({
          name: connectorChatBuilderInfo.connector_builder_name,
          stream: connectorChatBuilderInfo.connector_builder_stream,
          documentation_url,
          submit_form: connectorChatBuilderInfo.connector_builder_submit_form,
        })
      );
    }
  }
};

export const getConnectorChatBuilderParamsFromStorage = (): Record<string, string> | undefined => {
  const connectorChatBuilderParams = sessionStorage.getItem(CONNECTOR_CHAT_BUILDER_STORAGE_KEY);
  const parsedParams = connectorChatBuilderParams ? JSON.parse(connectorChatBuilderParams) : undefined;

  if (parsedParams?.submit_form) {
    parsedParams.submit_form = parsedParams.submit_form === "true";
  }

  return parsedParams;
};

export const clearConnectorChatBuilderStorage = (): void => {
  sessionStorage.removeItem(CONNECTOR_CHAT_BUILDER_STORAGE_KEY);
};

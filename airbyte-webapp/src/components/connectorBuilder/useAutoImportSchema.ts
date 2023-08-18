import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { useBuilderWatch } from "./types";

// only auto import schema if it is enabled for the provided stream and connector is in draft mode
export const useAutoImportSchema = (streamIndex: number) => {
  const { displayedVersion } = useConnectorBuilderFormState();
  const streams = useBuilderWatch("formValues.streams");

  return streams[streamIndex]?.autoImportSchema && displayedVersion === undefined;
};

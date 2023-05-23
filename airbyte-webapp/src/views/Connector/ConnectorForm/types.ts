// TODO: This needs to be converted to interface, but has int he current state a problem with index signatures

import { DestinationDefinitionRead, SourceDefinitionRead } from "core/request/AirbyteClient";

// eslint-disable-next-line @typescript-eslint/consistent-type-definitions
export type ConnectorFormValues<T = unknown> = {
  name: string;
  connectionConfiguration: T;
};

// The whole ConnectorCard form values
export type ConnectorCardValues = { serviceType: string } & ConnectorFormValues;

export type DestinationConnectorCard = Pick<
  DestinationDefinitionRead,
  "destinationDefinitionId" | "name" | "icon" | "releaseStage"
>;
export type SourceConnectorCard = Pick<SourceDefinitionRead, "sourceDefinitionId" | "name" | "icon" | "releaseStage">;

export type SuggestedConnector = (
  | Omit<DestinationConnectorCard, "destinationDefinitionId">
  | Omit<SourceConnectorCard, "sourceDefinitionId">
) & { connectorDefinitionId: string };

import {
  DestinationDefinitionRead,
  DestinationDefinitionSpecificationRead,
  DestinationRead,
  SourceDefinitionRead,
  SourceDefinitionSpecificationRead,
  SourceRead,
} from "../../request/AirbyteClient";

export type ConnectorDefinition = SourceDefinitionRead | DestinationDefinitionRead;

export type SourceDefinitionSpecificationDraft = Pick<
  SourceDefinitionSpecificationRead,
  "documentationUrl" | "connectionSpecification" | "authSpecification" | "advancedAuth"
>;

export type ConnectorDefinitionSpecification =
  | DestinationDefinitionSpecificationRead
  | SourceDefinitionSpecificationRead;

export type ConnectorT = DestinationRead | SourceRead;

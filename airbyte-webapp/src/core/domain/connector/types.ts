import {
  DestinationDefinitionRead,
  DestinationDefinitionSpecificationRead,
  DestinationRead,
  SourceDefinitionRead,
  SourceDefinitionSpecificationRead,
  SourceRead,
} from "core/api/types/AirbyteClient";

export type ConnectorDefinition = SourceDefinitionRead | DestinationDefinitionRead;

export type SourceDefinitionSpecificationDraft = Pick<
  SourceDefinitionSpecificationRead,
  "documentationUrl" | "connectionSpecification" | "advancedAuth"
>;

export type ConnectorDefinitionSpecification =
  | DestinationDefinitionSpecificationRead
  | SourceDefinitionSpecificationRead;

export type ConnectorT = DestinationRead | SourceRead;

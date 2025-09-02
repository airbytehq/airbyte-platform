import {
  DestinationDefinitionRead,
  DestinationDefinitionSpecificationRead,
  DestinationRead,
  EnterpriseConnectorStub,
  SourceDefinitionRead,
  SourceDefinitionSpecificationRead,
  SourceRead,
} from "core/api/types/AirbyteClient";

export interface EnterpriseSources {
  enterpriseSourceDefinitions: EnterpriseConnectorStubType[];
  enterpriseSourceDefinitionsMap: Map<string, EnterpriseConnectorStubType>;
}

export interface EnterpriseConnectorStubType extends EnterpriseConnectorStub {
  isEnterprise: true;
}

export type ConnectorDefinition = SourceDefinitionRead | DestinationDefinitionRead;
export type ConnectorDefinitionOrEnterpriseStub = ConnectorDefinition | EnterpriseConnectorStubType;

export type SourceDefinitionSpecificationDraft = Pick<
  SourceDefinitionSpecificationRead,
  "documentationUrl" | "connectionSpecification" | "advancedAuth" | "advancedAuthGlobalCredentialsAvailable"
>;

export type ConnectorDefinitionSpecificationRead =
  | DestinationDefinitionSpecificationRead
  | SourceDefinitionSpecificationRead;

export type ConnectorT = DestinationRead | SourceRead;

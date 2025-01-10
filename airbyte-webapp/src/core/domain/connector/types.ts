import {
  DestinationDefinitionRead,
  DestinationDefinitionSpecificationRead,
  DestinationRead,
  EnterpriseSourceStub,
  SourceDefinitionRead,
  SourceDefinitionSpecificationRead,
  SourceRead,
} from "core/api/types/AirbyteClient";

export interface EnterpriseSources {
  enterpriseSourceDefinitions: EnterpriseSourceStubType[];
  enterpriseSourceDefinitionsMap: Map<string, EnterpriseSourceStubType>;
}

export interface EnterpriseSourceStubType extends EnterpriseSourceStub {
  isEnterprise: true;
}

export type ConnectorDefinition = SourceDefinitionRead | DestinationDefinitionRead;
export type ConnectorDefinitionOrEnterpriseStub = ConnectorDefinition | EnterpriseSourceStubType;

export type SourceDefinitionSpecificationDraft = Pick<
  SourceDefinitionSpecificationRead,
  "documentationUrl" | "connectionSpecification" | "advancedAuth" | "advancedAuthGlobalCredentialsAvailable"
>;

export type ConnectorDefinitionSpecificationRead =
  | DestinationDefinitionSpecificationRead
  | SourceDefinitionSpecificationRead;

export type ConnectorT = DestinationRead | SourceRead;

import React from "react";

import { getSupportRevokingTokensConnectorIds } from "core/domain/connector/constants";
import { isSourceDefinitionSpecificationDraft } from "core/domain/connector/source";
import { FeatureItem, IfFeatureEnabled } from "core/services/features";
import { useConnectorForm } from "views/Connector/ConnectorForm/connectorFormContext";

import { AuthButton } from "./AuthButton";
import { RevokeButton } from "./RevokeButton";
import { FlexContainer } from "../../../../../../components/ui/Flex";
import { ConnectorSpecification } from "../../../../../../core/domain/connector";
import { useAuthentication } from "../../../useAuthentication";
import { SectionContainer } from "../SectionContainer";

export const AuthSection: React.FC = () => {
  const { selectedConnectorDefinitionSpecification, connectorId } = useConnectorForm();
  const { hasAuthFieldValues } = useAuthentication();
  if (
    !selectedConnectorDefinitionSpecification ||
    isSourceDefinitionSpecificationDraft(selectedConnectorDefinitionSpecification)
  ) {
    return null;
  }
  const definitionId = ConnectorSpecification.id(selectedConnectorDefinitionSpecification);
  const supportsRevokingTokens = getSupportRevokingTokensConnectorIds().includes(definitionId);

  return (
    <IfFeatureEnabled feature={FeatureItem.AllowOAuthConnector}>
      <SectionContainer>
        <FlexContainer direction="row" justifyContent="space-between" alignItems="center">
          <AuthButton selectedConnectorDefinitionSpecification={selectedConnectorDefinitionSpecification} />
          {supportsRevokingTokens && hasAuthFieldValues && connectorId && (
            <RevokeButton
              sourceId={connectorId}
              selectedConnectorDefinitionSpecification={selectedConnectorDefinitionSpecification}
            />
          )}
        </FlexContainer>
      </SectionContainer>
    </IfFeatureEnabled>
  );
};

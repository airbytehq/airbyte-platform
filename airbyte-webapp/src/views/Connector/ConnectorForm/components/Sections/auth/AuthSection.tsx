import React from "react";
import { FormattedMessage } from "react-intl";

import { getSupportRevokingTokensConnectorIds } from "core/domain/connector/constants";
import { isSourceDefinitionSpecificationDraft } from "core/domain/connector/source";
import { FeatureItem, IfFeatureEnabled } from "core/services/features";
import { useConnectorForm } from "views/Connector/ConnectorForm/connectorFormContext";

import { AuthButton } from "./AuthButton";
import { RevokeButton } from "./RevokeButton";
import { FlexContainer } from "../../../../../../components/ui/Flex";
import { Message } from "../../../../../../components/ui/Message";
import { ConnectorSpecification } from "../../../../../../core/domain/connector";
import { OAUTH_REDIRECT_URL } from "../../../../../../hooks/services/useConnectorAuth";
import { useAuthentication } from "../../../useAuthentication";
import { SectionContainer } from "../SectionContainer";

export const AuthSection: React.FC = () => {
  const { selectedConnectorDefinitionSpecification, connectorId } = useConnectorForm();
  const { hasAuthFieldValues, shouldShowRedirectUrlTooltip } = useAuthentication();
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
      {shouldShowRedirectUrlTooltip && (
        <Message text={<FormattedMessage id="connectorForm.redirectUrl" values={{ url: OAUTH_REDIRECT_URL }} />} />
      )}
    </IfFeatureEnabled>
  );
};

import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";

import { useConnectorForm } from "area/connector/components/ConnectorForm/connectorFormContext";
import { useAuthentication } from "area/connector/components/ConnectorForm/useAuthentication";
import { OAUTH_REDIRECT_URL } from "area/connector/utils/useConnectorAuth";
import { ConnectorSpecification } from "core/domain/connector";
import { getSupportRevokingTokensConnectorIds } from "core/domain/connector/constants";
import { isSourceDefinitionSpecificationDraft } from "core/domain/connector/source";

import { AuthButton } from "./AuthButton";
import { RevokeButton } from "./RevokeButton";
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
    <SectionContainer>
      <FlexContainer direction="column" gap="xl">
        <FlexContainer direction="row" justifyContent="space-between" alignItems="center">
          <AuthButton selectedConnectorDefinitionSpecification={selectedConnectorDefinitionSpecification} />
          {supportsRevokingTokens && hasAuthFieldValues && connectorId && (
            <RevokeButton
              sourceId={connectorId}
              selectedConnectorDefinitionSpecification={selectedConnectorDefinitionSpecification}
            />
          )}
        </FlexContainer>
        {shouldShowRedirectUrlTooltip && (
          <Message text={<FormattedMessage id="connectorForm.redirectUrl" values={{ url: OAUTH_REDIRECT_URL }} />} />
        )}
      </FlexContainer>
    </SectionContainer>
  );
};

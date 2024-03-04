import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";

import { ConnectorDefinitionSpecification } from "core/domain/connector";

import { useFormOauthRevocationAdapter } from "./useOauthRevocationAdapter";
import { FlexContainer } from "../../../../../../components/ui/Flex";
import { useConnectorForm } from "../../../connectorFormContext";

interface RevokeButtonProps {
  sourceId: string;
  selectedConnectorDefinitionSpecification: ConnectorDefinitionSpecification;
}

export const RevokeButton: React.FC<RevokeButtonProps> = ({ sourceId, selectedConnectorDefinitionSpecification }) => {
  const { selectedConnectorDefinition } = useConnectorForm();
  const { loading, run } = useFormOauthRevocationAdapter(
    sourceId,
    selectedConnectorDefinitionSpecification,
    selectedConnectorDefinition
  );

  if (!selectedConnectorDefinition) {
    console.error("Entered oauth revocation flow while no supported connector is selected");
    return null;
  }

  const buttonLabel = <FormattedMessage id="connectorForm.revoke" />;
  return (
    <FlexContainer alignItems="center">
      <Button
        variant="danger"
        isLoading={loading}
        type="button"
        data-id="oauth-revoke-button"
        onClick={run}
        icon={<Icon type="disabled" />}
        size="sm"
      >
        {buttonLabel}
      </Button>
    </FlexContainer>
  );
};

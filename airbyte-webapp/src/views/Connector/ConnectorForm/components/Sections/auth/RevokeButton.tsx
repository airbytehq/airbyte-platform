import { faBan } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";

import { ConnectorDefinitionSpecification } from "core/domain/connector";

import { useFormOauthRevocationAdapter } from "./useOauthRevocationAdapter";
import { FlexContainer } from "../../../../../../components/ui/Flex";
import { useConnectorForm } from "../../../connectorFormContext";

export const RevokeButton: React.FC<{
  selectedConnectorDefinitionSpecification: ConnectorDefinitionSpecification;
}> = ({ selectedConnectorDefinitionSpecification }) => {
  const { selectedConnectorDefinition } = useConnectorForm();
  const { loading, run } = useFormOauthRevocationAdapter(
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
        icon={<FontAwesomeIcon icon={faBan} />}
        size="sm"
      >
        {buttonLabel}
      </Button>
    </FlexContainer>
  );
};

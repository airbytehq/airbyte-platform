import React, { useImperativeHandle, useRef, useState } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectorIds } from "area/connector/utils";
import { ConnectorDefinitionSpecificationRead, ConnectorSpecification } from "core/domain/connector";
import { useIsAirbyteEmbeddedContext } from "core/services/embedded";

import styles from "./AuthButton.module.scss";
import { GoogleAuthButton } from "./GoogleAuthButton";
import QuickBooksAuthButton from "./QuickBooksAuthButton";
import { useFormOauthAdapter, useFormOauthAdapterBuilder } from "./useOauthFlowAdapter";
import { useConnectorForm } from "../../../connectorFormContext";
import { useAuthentication } from "../../../useAuthentication";

function isGoogleConnector(connectorDefinitionId: string): boolean {
  return (
    [
      ConnectorIds.Sources.GoogleAds,
      ConnectorIds.Sources.GoogleAnalyticsUniversalAnalytics,
      ConnectorIds.Sources.GoogleDirectory,
      ConnectorIds.Sources.GoogleSearchConsole,
      ConnectorIds.Sources.GoogleSheets,
      ConnectorIds.Sources.GoogleWorkspaceAdminReports,
      ConnectorIds.Sources.YouTubeAnalytics,
      ConnectorIds.Destinations.GoogleSheets,
      // TODO: revert me
      ConnectorIds.Sources.YouTubeAnalyticsBusiness,
      //
    ] as string[]
  ).includes(connectorDefinitionId);
}

function getButtonComponent(connectorDefinitionId: string) {
  if (isGoogleConnector(connectorDefinitionId)) {
    return GoogleAuthButton;
  }
  if (connectorDefinitionId === ConnectorIds.Sources.QuickBooks) {
    return QuickBooksAuthButton;
  }
  return Button;
}

function getAuthenticateMessageId(connectorDefinitionId: string): string {
  if (isGoogleConnector(connectorDefinitionId)) {
    return "connectorForm.signInWithGoogle";
  }
  return "connectorForm.authenticate";
}

export const AuthButton: React.FC<{
  selectedConnectorDefinitionSpecification: ConnectorDefinitionSpecificationRead;
}> = ({ selectedConnectorDefinitionSpecification }) => {
  const { selectedConnectorDefinition } = useConnectorForm();
  const isAirbyteEmbedded = useIsAirbyteEmbeddedContext();

  const { hiddenAuthFieldErrors, manualOAuthMode, toggleManualOAuthMode } = useAuthentication();
  const authRequiredError = Object.values(hiddenAuthFieldErrors).includes("required");

  const { loading, done, run } = useFormOauthAdapter(selectedConnectorDefinitionSpecification);

  const definitionId = ConnectorSpecification.id(selectedConnectorDefinitionSpecification);
  const Component = getButtonComponent(definitionId);

  if (manualOAuthMode) {
    return (
      <FlexContainer direction="column" gap="lg" alignItems="flex-start">
        <Button variant="clear" icon="chevronLeft" iconSize="lg" onClick={toggleManualOAuthMode}>
          <FormattedMessage id="connectorForm.manualAuth.back" />
        </Button>
      </FlexContainer>
    );
  }

  return (
    <FlexContainer direction="column" gap="lg" alignItems="flex-start">
      <FlexContainer gap="lg" alignItems="center" justifyContent="space-between">
        <Component isLoading={loading} type="button" data-testid="oauth-button" onClick={run}>
          {done ? (
            <FormattedMessage id="connectorForm.reauthenticate" />
          ) : (
            <FormattedMessage
              id={getAuthenticateMessageId(definitionId)}
              values={{ connector: selectedConnectorDefinition.name }}
            />
          )}
        </Component>

        {!isAirbyteEmbedded && (
          <Button variant="clear" onClick={toggleManualOAuthMode}>
            <FormattedMessage id="connectorForm.manualAuth.toggle" />
          </Button>
        )}
      </FlexContainer>

      {authRequiredError && (
        <Text as="div" size="lg" className={styles.error}>
          <FormattedMessage id="connectorForm.authenticate.required" />
        </Text>
      )}
    </FlexContainer>
  );
};

export const AuthButtonBuilderDeprecated = React.forwardRef<
  HTMLDivElement | null,
  {
    builderProjectId: string;
    onComplete: (authPayload: Record<string, unknown>) => void;
    onClick?: () => void;
    disabled?: boolean;
  }
>(({ builderProjectId, onComplete, onClick, disabled }, ref) => {
  const { loading, run } = useFormOauthAdapterBuilder(builderProjectId, onComplete);
  const [isAccented, setIsAccented] = useState(false);

  const flexRef = useRef<HTMLDivElement>(null);

  useImperativeHandle(
    ref,
    () =>
      new Proxy(flexRef.current!, {
        get(target, prop, receiver) {
          if (prop === "scrollIntoView") {
            const fn: HTMLElement["scrollIntoView"] = (...args) => {
              target.scrollIntoView(...args);
              setIsAccented(true);
            };
            return fn;
          }
          return Reflect.get(target, prop, receiver);
        },
      })
  );

  return (
    <FlexContainer alignItems="center" ref={flexRef}>
      <div className={isAccented ? styles.accented__container : undefined}>
        <Button
          disabled={disabled}
          isLoading={loading}
          type="button"
          data-testid="oauth-button"
          onClick={() => {
            setIsAccented(false);
            (onClick ?? run)();
          }}
          className={isAccented ? styles.accented__button : undefined}
        >
          <FormattedMessage id="connectorBuilder.authentication.oauthButton.label" />
        </Button>
      </div>
    </FlexContainer>
  );
});
AuthButtonBuilderDeprecated.displayName = "AuthButtonBuilderDeprecated";

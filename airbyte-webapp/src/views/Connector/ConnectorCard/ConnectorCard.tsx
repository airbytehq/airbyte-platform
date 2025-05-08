import React, { useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
// This import should be refactored to use our custom useLocalStorage hook, but requires a larger refactor
// eslint-disable-next-line no-restricted-imports
import { useLocalStorage } from "react-use";

import { Box } from "components/ui/Box";
import { Collapsible } from "components/ui/Collapsible";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";
import { Pre } from "components/ui/Pre";
import { Spinner } from "components/ui/Spinner";

import { useAirbyteCloudIps } from "area/connector/utils/useAirbyteCloudIps";
import { ErrorWithJobInfo, useCreateConfigTemplate, useCurrentWorkspace } from "core/api";
import { DestinationRead, SourceRead, SupportLevel } from "core/api/types/AirbyteClient";
import {
  Connector,
  ConnectorDefinition,
  ConnectorDefinitionSpecificationRead,
  ConnectorSpecification,
  ConnectorT,
} from "core/domain/connector";
import { isCloudApp } from "core/utils/app";
import { generateMessageFromError } from "core/utils/errorStatusMessage";
import { links } from "core/utils/links";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useExperiment } from "hooks/services/Experiment";
import { ConnectorCardValues, ConnectorForm, ConnectorFormValues } from "views/Connector/ConnectorForm";

import { Controls } from "./components/Controls";
import ShowLoadingMessage from "./components/ShowLoadingMessage";
import styles from "./ConnectorCard.module.scss";
import { useAnalyticsTrackFunctions } from "./useAnalyticsTrackFunctions";
import { useTestConnector } from "./useTestConnector";
import { useDocumentationPanelContext } from "../ConnectorDocumentationLayout/DocumentationPanelContext";
import { WarningMessage } from "../ConnectorForm/components/WarningMessage";

// TODO: need to clean up the ConnectorCard and ConnectorForm props,
// since some of props are used in both components, and some of them used just as a prop-drill
// https://github.com/airbytehq/airbyte/issues/18553
interface ConnectorCardBaseProps {
  title?: string;
  headerBlock?: React.ReactNode;
  description?: React.ReactNode;
  full?: boolean;
  onSubmit: (values: ConnectorCardValues) => Promise<void> | void;
  reloadConfig?: () => void;
  onDeleteClick?: () => void;
  onConnectorDefinitionSelect?: (id: string) => void;
  availableConnectorDefinitions: ConnectorDefinition[];
  supportLevel?: SupportLevel;

  // used in ConnectorCard and ConnectorForm
  formType: "source" | "destination";
  /**
   * id of the selected connector definition id - might be available even if the specification is not loaded yet
   * */
  selectedConnectorDefinitionId: string | null;
  selectedConnectorDefinitionSpecification?: ConnectorDefinitionSpecificationRead;
  isEditMode?: boolean;

  // used in ConnectorForm
  formId?: string;
  fetchingConnectorError?: Error | null;
  isLoading?: boolean;
  leftFooterSlot?: React.ReactNode;
}

interface ConnectorCardCreateProps extends ConnectorCardBaseProps {
  isEditMode?: false;
}

interface ConnectorCardEditProps extends ConnectorCardBaseProps {
  isEditMode: true;
  connector: ConnectorT;
}

const getConnectorId = (connectorRead: DestinationRead | SourceRead) => {
  return "sourceId" in connectorRead ? connectorRead.sourceId : connectorRead.destinationId;
};

/**
 * When creating an Airbyte Embedded Template, we need to set the authentication values to mock values.
 * These are overridden when the user creates a Source using the PartialUserCreateForm, but placeholders
 * are required in order for the template to be created + rendered properly.
 */

const prepareOauthFieldsForTemplate = (
  partialDefaultConfig: Record<string, unknown>,
  advancedAuth: ConnectorDefinitionSpecificationRead["advancedAuth"]
) => {
  if (advancedAuth?.oauthConfigSpecification) {
    // Set auth_type based on predicate
    if (advancedAuth.predicateKey && advancedAuth.predicateValue) {
      const authTypePath = advancedAuth.predicateKey;
      let current = partialDefaultConfig;
      for (let i = 0; i < authTypePath.length - 1; i++) {
        if (!current[authTypePath[i]]) {
          current[authTypePath[i]] = {};
        }
        current = current[authTypePath[i]] as Record<string, unknown>;
      }
      current[authTypePath[authTypePath.length - 1]] = advancedAuth.predicateValue;
    }

    // Handle all three OAuth specifications
    const specs = [
      advancedAuth.oauthConfigSpecification.completeOAuthOutputSpecification,
      advancedAuth.oauthConfigSpecification.completeOAuthServerInputSpecification,
      advancedAuth.oauthConfigSpecification.completeOAuthServerOutputSpecification,
    ].filter((spec): spec is NonNullable<typeof spec> => spec !== undefined);

    specs.forEach((spec) => {
      const typedSpec = spec as unknown as {
        properties: Record<string, { path_in_connector_config?: string[] }>;
      };
      if (typedSpec.properties) {
        Object.entries(typedSpec.properties).forEach(([_, property]) => {
          if (property.path_in_connector_config) {
            const pathParts = property.path_in_connector_config;
            if (pathParts.length > 0) {
              let current = partialDefaultConfig;
              for (let i = 0; i < pathParts.length - 1; i++) {
                if (!current[pathParts[i]]) {
                  current[pathParts[i]] = {};
                }
                current = current[pathParts[i]] as Record<string, unknown>;
              }
              current[pathParts[pathParts.length - 1]] = "mock-string";
            }
          }
        });
      }
    });
  }
};

export const ConnectorCard: React.FC<ConnectorCardCreateProps | ConnectorCardEditProps> = ({
  onSubmit,
  onDeleteClick,
  selectedConnectorDefinitionId,
  fetchingConnectorError,
  reloadConfig,
  headerBlock,
  supportLevel,
  leftFooterSlot = null,
  ...props
}) => {
  const canEditConnector = useGeneratedIntent(Intent.CreateOrEditConnector);
  const [errorStatusRequest, setErrorStatusRequest] = useState<Error | null>(null);
  const { formatMessage } = useIntl();
  const { organizationId, workspaceId } = useCurrentWorkspace();
  const { mutate: createConfigTemplate } = useCreateConfigTemplate();
  const isTemplateCreateButtonEnabled = useExperiment("embedded.templateCreateButton");
  const canCreateConfigTemplate = useGeneratedIntent(Intent.CreateOrEditConnection);
  const showCreateTemplateButton = isTemplateCreateButtonEnabled && canCreateConfigTemplate;

  const { setDocumentationPanelOpen, setSelectedConnectorDefinition } = useDocumentationPanelContext();
  const {
    testConnector,
    isTestConnectionInProgress,
    onStopTesting,
    error,
    reset,
    isSuccess: connectionTestSuccess,
  } = useTestConnector(props);
  const { trackTestConnectorFailure, trackTestConnectorSuccess, trackTestConnectorStarted } =
    useAnalyticsTrackFunctions(props.formType);

  useEffect(() => {
    // Whenever the selected connector changed, reset the check connection call and other errors
    reset();
    setErrorStatusRequest(null);
  }, [props.selectedConnectorDefinitionSpecification, reset]);

  const { selectedConnectorDefinitionSpecification, availableConnectorDefinitions, isEditMode } = props;

  const selectedConnectorDefinitionSpecificationId =
    selectedConnectorDefinitionId ||
    (selectedConnectorDefinitionSpecification && ConnectorSpecification.id(selectedConnectorDefinitionSpecification));

  const selectedConnectorDefinition = useMemo(() => {
    const definition = availableConnectorDefinitions.find(
      (s) => Connector.id(s) === selectedConnectorDefinitionSpecificationId
    );
    if (!definition) {
      throw new Error(`Connector definition not found for id: ${selectedConnectorDefinitionSpecificationId}`);
    }
    return definition;
  }, [availableConnectorDefinitions, selectedConnectorDefinitionSpecificationId]);
  const advancedAuth = selectedConnectorDefinitionSpecification?.advancedAuth;

  // Handle Doc panel
  useEffect(() => {
    if (!selectedConnectorDefinition) {
      return;
    }

    setDocumentationPanelOpen(true);
    setSelectedConnectorDefinition(selectedConnectorDefinition);
  }, [
    selectedConnectorDefinitionSpecification,
    selectedConnectorDefinition,
    setDocumentationPanelOpen,
    setSelectedConnectorDefinition,
  ]);

  const testConnectorWithTracking = async (connectorCardValues?: ConnectorCardValues) => {
    trackTestConnectorStarted(selectedConnectorDefinition);
    try {
      const response = await testConnector(connectorCardValues);
      trackTestConnectorSuccess(selectedConnectorDefinition);
      return response;
    } catch (e) {
      trackTestConnectorFailure(selectedConnectorDefinition, ErrorWithJobInfo.getJobInfo(e), e.message);
      throw e;
    }
  };

  const handleTestConnector = async (values?: ConnectorCardValues) => {
    setErrorStatusRequest(null);
    try {
      await testConnectorWithTracking(values);
    } catch (e) {
      setErrorStatusRequest(e);
      throw e;
    }
  };

  const onHandleSubmit = async (values: ConnectorFormValues) => {
    if (!selectedConnectorDefinition) {
      return;
    }
    setErrorStatusRequest(null);

    //  combine the "ConnectorFormValues" and serviceType to make "ConnectorFormValues"
    const connectorCardValues: ConnectorCardValues = {
      ...values,
      serviceType: Connector.id(selectedConnectorDefinition),
    };

    try {
      const response = await testConnectorWithTracking(connectorCardValues);
      if (response.jobInfo.connectorConfigurationUpdated && reloadConfig) {
        reloadConfig();
      } else {
        await onSubmit(connectorCardValues);
      }
    } catch (e) {
      setErrorStatusRequest(e);
      // keep throwing the exception to inform the component the submit did not go through
      throw e;
    }
  };

  const job = ErrorWithJobInfo.getJobInfo(errorStatusRequest);

  const connector = isEditMode ? props.connector : undefined;
  const connectorId = connector ? getConnectorId(connector) : undefined;

  const isConnectorEntitled = connector?.isEntitled === true;

  // Fill form with existing connector values otherwise set the default service name
  const formValues = useMemo(() => {
    if (isEditMode && connector) {
      return connector;
    }
    return { name: selectedConnectorDefinition.name };
  }, [isEditMode, connector, selectedConnectorDefinition.name]);

  return (
    <ConnectorForm
      canEdit={canEditConnector && (isConnectorEntitled || !connector)}
      trackDirtyChanges
      headerBlock={
        <FlexContainer direction="column" className={styles.header}>
          {headerBlock}
          <WarningMessage supportLevel={supportLevel} />
          {props.isLoading && (
            <div className={styles.loaderContainer}>
              <Spinner />
              <div className={styles.loadingMessage}>
                <ShowLoadingMessage connector={selectedConnectorDefinition.name} />
              </div>
            </div>
          )}
          {fetchingConnectorError && (
            <Message
              type="error"
              text={<FormattedMessage id="form.failedFetchingConnector" />}
              secondaryText={<FormattedMessage id="form.tryAgain" />}
            />
          )}
        </FlexContainer>
      }
      // Causes the whole ConnectorForm to be unmounted and a new instance mounted whenever the connector type changes.
      // That way we carry less state around inside it, preventing any state from one connector type from affecting another
      // connector type's form in any way.
      // Also re-mount the connector form if the spec changes
      key={
        selectedConnectorDefinition &&
        Connector.id(selectedConnectorDefinition) + (selectedConnectorDefinitionSpecification ? "true" : "false")
      }
      {...props}
      selectedConnectorDefinition={selectedConnectorDefinition}
      selectedConnectorDefinitionSpecification={selectedConnectorDefinitionSpecification}
      isTestConnectionInProgress={isTestConnectionInProgress}
      connectionTestSuccess={connectionTestSuccess}
      onSubmit={onHandleSubmit}
      formValues={formValues}
      connectorId={connectorId}
      renderFooter={({ dirty, isSubmitting, isValid, resetConnectorForm, getValues }) =>
        selectedConnectorDefinitionSpecification && (
          <>
            {isCloudApp() &&
              selectedConnectorDefinition &&
              "sourceDefinitionId" in selectedConnectorDefinition &&
              selectedConnectorDefinition.sourceType === "database" && <AllowlistIpBanner connectorId={connectorId} />}
            <Controls
              isEditMode={Boolean(isEditMode)}
              isTestConnectionInProgress={isTestConnectionInProgress}
              onCancelTesting={onStopTesting}
              isSubmitting={isSubmitting || isTestConnectionInProgress}
              errorMessage={error && generateMessageFromError(error, formatMessage)}
              formType={props.formType}
              hasDefinition={Boolean(selectedConnectorDefinitionId)}
              onRetestClick={() => {
                if (!selectedConnectorDefinitionId) {
                  return;
                }
                handleTestConnector(
                  isEditMode ? undefined : { ...getValues(), serviceType: selectedConnectorDefinitionId }
                );
              }}
              onDeleteClick={onDeleteClick}
              isValid={isValid}
              dirty={dirty}
              job={job ?? undefined}
              onCancelClick={() => {
                resetConnectorForm();
              }}
              connectionTestSuccess={connectionTestSuccess}
              leftSlot={leftFooterSlot}
              onCopyConfig={() => {
                const values = getValues();
                const definitionId = selectedConnectorDefinition ? Connector.id(selectedConnectorDefinition) : "";
                return {
                  name: values.name,
                  workspaceId,
                  definitionId,
                  config: values.connectionConfiguration as Record<string, unknown>,
                  schema: selectedConnectorDefinitionSpecification?.connectionSpecification,
                };
              }}
              onCreateConfigTemplate={
                showCreateTemplateButton
                  ? () => {
                      const values = getValues();
                      const definitionId = selectedConnectorDefinition ? Connector.id(selectedConnectorDefinition) : "";
                      // we do not want to include the credentials property in the default values we send to the template create call.
                      // We want to use the API default behavior, which is to enable oauth where possible, and otherwise allow whatever
                      // the connector allows
                      const partialDefaultConfig = { ...values.connectionConfiguration } as Record<string, unknown>;

                      prepareOauthFieldsForTemplate(partialDefaultConfig, advancedAuth);

                      createConfigTemplate({
                        organizationId,
                        actorDefinitionId: definitionId,
                        partialDefaultConfig,
                      });
                    }
                  : undefined
              }
            />
          </>
        )
      }
    />
  );
};

const AllowlistIpBanner = ({ connectorId }: { connectorId: string | undefined }) => {
  const { formatMessage } = useIntl();
  const [allowlistIpsOpen, setAllowlistIpsOpen] = useLocalStorage(
    connectorId ? `allowlistIpsOpen_${connectorId}` : "allowlistIpsOpen",
    true
  );

  return (
    <Message
      type="info"
      text={
        <FormattedMessage
          id="connectorForm.allowlistIp.message"
          values={{
            a: (node: React.ReactNode) => (
              <a href={links.cloudAllowlistIPsLink} target="_blank" rel="noreferrer">
                {node}
              </a>
            ),
          }}
        />
      }
    >
      <Box p="lg">
        <Collapsible
          className={styles.collapsibleIpAddresses}
          label={formatMessage({ id: "connectorForm.allowlistIp.addressesLabel" })}
          initiallyOpen={connectorId ? allowlistIpsOpen ?? true : true}
          onClick={(newOpenState) => setAllowlistIpsOpen(newOpenState)}
        >
          <Pre>{useAirbyteCloudIps().join("\n")}</Pre>
        </Collapsible>
      </Box>
    </Message>
  );
};

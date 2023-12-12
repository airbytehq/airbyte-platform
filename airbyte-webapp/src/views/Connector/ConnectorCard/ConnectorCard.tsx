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
import { LogsRequestError } from "core/api";
import { DestinationRead, SourceRead, SupportLevel, SynchronousJobRead } from "core/api/types/AirbyteClient";
import {
  Connector,
  ConnectorDefinition,
  ConnectorDefinitionSpecification,
  ConnectorSpecification,
  ConnectorT,
} from "core/domain/connector";
import { isCloudApp } from "core/utils/app";
import { generateMessageFromError } from "core/utils/errorStatusMessage";
import { links } from "core/utils/links";
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
  title?: React.ReactNode;
  headerBlock?: React.ReactNode;
  description?: React.ReactNode;
  full?: boolean;
  jobInfo?: SynchronousJobRead | null;
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
  selectedConnectorDefinitionSpecification?: ConnectorDefinitionSpecification;
  isEditMode?: boolean;

  // used in ConnectorForm
  formId?: string;
  fetchingConnectorError?: Error | null;
  isLoading?: boolean;
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

export const ConnectorCard: React.FC<ConnectorCardCreateProps | ConnectorCardEditProps> = ({
  jobInfo,
  onSubmit,
  onDeleteClick,
  selectedConnectorDefinitionId,
  fetchingConnectorError,
  reloadConfig,
  headerBlock,
  supportLevel,
  ...props
}) => {
  const [errorStatusRequest, setErrorStatusRequest] = useState<Error | null>(null);

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

  const selectedConnectorDefinition = useMemo(
    () => availableConnectorDefinitions.find((s) => Connector.id(s) === selectedConnectorDefinitionSpecificationId),
    [availableConnectorDefinitions, selectedConnectorDefinitionSpecificationId]
  );

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
      trackTestConnectorFailure(selectedConnectorDefinition, LogsRequestError.extractJobInfo(e), e.message);
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

  const job = jobInfo || LogsRequestError.extractJobInfo(errorStatusRequest);

  const connector = isEditMode ? props.connector : undefined;
  const connectorId = connector ? getConnectorId(connector) : undefined;

  // Fill form with existing connector values otherwise set the default service name
  const formValues = useMemo(
    () => (isEditMode && connector ? connector : { name: selectedConnectorDefinition?.name }),
    [isEditMode, connector, selectedConnectorDefinition?.name]
  );

  return (
    <ConnectorForm
      trackDirtyChanges
      headerBlock={
        <FlexContainer direction="column" className={styles.header}>
          {headerBlock}
          <WarningMessage supportLevel={supportLevel} />
          {props.isLoading && (
            <div className={styles.loaderContainer}>
              <Spinner />
              <div className={styles.loadingMessage}>
                <ShowLoadingMessage connector={selectedConnectorDefinition?.name} />
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
              errorMessage={error && generateMessageFromError(error)}
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
              job={job ? job : undefined}
              onCancelClick={() => {
                resetConnectorForm();
              }}
              connectionTestSuccess={connectionTestSuccess}
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

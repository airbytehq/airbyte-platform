import React, { useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
// This import should be refactored to use our custom useLocalStorage hook, but requires a larger refactor
// eslint-disable-next-line no-restricted-imports
import { useLocalStorage } from "react-use";

import { Box } from "components/ui/Box";
import { Collapsible } from "components/ui/Collapsible";
import { FlexContainer } from "components/ui/Flex";
import { LogoAnimation } from "components/ui/LoadingPage/LogoAnimation";
import { Message } from "components/ui/Message";
import { Pre } from "components/ui/Pre";

import { ConnectorCardValues, ConnectorForm, ConnectorFormValues } from "area/connector/components/ConnectorForm";
import { useAirbyteCloudIpsByDataplane } from "area/connector/utils/useAirbyteCloudIpsByDataplane";
import { CommandErrorWithJobInfo, useTestConnectorCommand } from "core/api";
import { DestinationRead, SourceRead, SupportLevel } from "core/api/types/AirbyteClient";
import {
  Connector,
  ConnectorDefinition,
  ConnectorDefinitionSpecificationRead,
  ConnectorSpecification,
  ConnectorT,
} from "core/domain/connector";
import { useNotificationService } from "core/services/Notification";
import { useIsCloudApp } from "core/utils/app";
import { generateMessageFromError } from "core/utils/errorStatusMessage";
import { links } from "core/utils/links";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { ConnectorCardHeader } from "./components/ConnectorCardHeader";
import { Controls } from "./components/Controls";
import SlowLoadingMessage from "./components/SlowLoadingMessage";
import styles from "./ConnectorCard.module.scss";
import { useAnalyticsTrackFunctions } from "./useAnalyticsTrackFunctions";
import { useDocumentationPanelContext } from "../ConnectorDocumentationLayout/DocumentationPanelContext";
import { WarningMessage } from "../ConnectorForm/components/WarningMessage";

// TODO: need to clean up the ConnectorCard and ConnectorForm props,
// since some of props are used in both components, and some of them used just as a prop-drill
// https://github.com/airbytehq/airbyte/issues/18553
interface ConnectorCardBaseProps<T extends ConnectorT> {
  title?: string;
  headerBlock?: React.ReactNode;
  description?: React.ReactNode;
  full?: boolean;
  onSubmit: (values: ConnectorCardValues) => Promise<void> | void;
  reloadConfig?: () => void;
  onDeleteClick?: () => void;
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
  preFooterSlot?: React.ReactNode;
  /** Reaches ConnectorForm through the props spread. Renders inside the form context, outside the footer. */
  formAutomation?: React.ReactNode;
  skipCheckConnection?: boolean;
  onSaveDraft?: (values: ConnectorCardValues, existingDraft?: T) => Promise<T>;
  onDraftPromoted?: (draft: T, values: ConnectorCardValues) => Promise<void> | void;
}

interface ConnectorCardCreateProps<T extends ConnectorT> extends ConnectorCardBaseProps<T> {
  isEditMode?: false;
}

interface ConnectorCardEditProps<T extends ConnectorT> extends ConnectorCardBaseProps<T> {
  isEditMode: true;
  connector: T;
}

const getConnectorId = (connectorRead: DestinationRead | SourceRead) => {
  return "sourceId" in connectorRead ? connectorRead.sourceId : connectorRead.destinationId;
};

const getConnectionConfigurationDefaults = (connectorDefinitionSpecification: ConnectorDefinitionSpecificationRead) => {
  const { properties = {}, required = [] } = connectorDefinitionSpecification.connectionSpecification ?? {};
  const props = properties as Record<string, { default?: unknown }>;
  return Object.fromEntries(
    Object.entries(props)
      .filter(
        ([key, property]) =>
          (required as string[]).includes(key) &&
          property &&
          typeof property === "object" &&
          "default" in property &&
          property.default !== undefined
      )
      .map(([key, property]) => [key, property.default])
  );
};

export const ConnectorCard = <T extends ConnectorT>({
  onSubmit,
  onDeleteClick,
  selectedConnectorDefinitionId,
  fetchingConnectorError,
  reloadConfig,
  headerBlock,
  supportLevel,
  leftFooterSlot = null,
  preFooterSlot = null,
  skipCheckConnection = false,
  onSaveDraft,
  onDraftPromoted,
  ...props
}: ConnectorCardCreateProps<T> | ConnectorCardEditProps<T>) => {
  const canEditConnector = useGeneratedIntent(
    props.formType === "source" ? Intent.CreateOrEditSource : Intent.CreateOrEditDestination
  );
  const [errorStatusRequest, setErrorStatusRequest] = useState<Error | null>(null);
  const [draftConnector, setDraftConnector] = useState<T | undefined>(
    props.isEditMode && props.connector.isDraft ? props.connector : undefined
  );
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();

  const { setDocumentationPanelOpen, setSelectedConnectorDefinition } = useDocumentationPanelContext();

  const {
    testConnector,
    isTestConnectionInProgress,
    onStopTesting,
    error,
    reset,
    isSuccess: connectionTestSuccess,
  } = useTestConnectorCommand(props);
  // A separate check for "Continue" between form sections, so its result never shows as the outcome of "Save & Test"
  const { testConnector: testSection } = useTestConnectorCommand({ formType: props.formType });
  const isCloudApp = useIsCloudApp();
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

  const testConnectorWithTracking = async (
    connectorCardValues?: ConnectorCardValues,
    connectorOverride?: ConnectorT
  ) => {
    trackTestConnectorStarted(selectedConnectorDefinition);
    try {
      const response = await testConnector(connectorCardValues, connectorOverride);
      if (response.status === "succeeded") {
        trackTestConnectorSuccess(selectedConnectorDefinition);
      } else {
        trackTestConnectorFailure(selectedConnectorDefinition, null, response.status);
      }
      return response;
    } catch (e) {
      trackTestConnectorFailure(selectedConnectorDefinition, CommandErrorWithJobInfo.getJobInfo(e), e.message);
      throw e;
    }
  };

  const checkSection = async (values: ConnectorFormValues) => {
    const output = await testSection({ ...values, serviceType: Connector.id(selectedConnectorDefinition) });
    // A failed check throws with its job info, but a check that was aborted resolves as failed
    if (output.status !== "succeeded") {
      throw new Error(formatMessage({ id: "connector.check.failed" }));
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

  const onHandleSubmit = async (values: ConnectorFormValues, shouldSkipCheckConnection = false) => {
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
      if (onSaveDraft) {
        const savedDraft = await onSaveDraft(connectorCardValues, draftConnector);
        setDraftConnector(savedDraft);
        const checkResult = await testConnectorWithTracking(undefined, savedDraft);
        if (checkResult.status !== "succeeded") {
          return;
        }
        await onDraftPromoted?.(savedDraft, connectorCardValues);
        return;
      }
      if (shouldSkipCheckConnection) {
        await onSubmit(connectorCardValues);
      } else {
        const response = await testConnectorWithTracking(connectorCardValues);
        if (response.connectorConfigurationUpdated && reloadConfig) {
          reloadConfig();
        } else {
          await onSubmit(connectorCardValues);
        }
      }
    } catch (e) {
      setErrorStatusRequest(e);
      registerNotification({
        id: "connector.submitFailed",
        type: "error",
        text: formatMessage({ id: "connectorForm.submitFailed" }, { name: values.name, error: String(e.message) }),
      });
      // keep throwing the exception to inform the component the submit did not go through
      throw e;
    }
  };

  const jobInfo = CommandErrorWithJobInfo.getJobInfo(errorStatusRequest);

  const connector = isEditMode ? props.connector : undefined;
  const connectorId = connector ? getConnectorId(connector) : undefined;

  const isConnectorEntitled = connector?.isEntitled === true;

  // Fill form with existing connector values otherwise set the default service name
  const formValues = useMemo(() => {
    if (isEditMode && connector) {
      return {
        ...connector,
        connectionConfiguration: {
          ...getConnectionConfigurationDefaults(selectedConnectorDefinitionSpecification!),
          ...connector.connectionConfiguration,
        },
      };
    }
    return { name: selectedConnectorDefinition.name };
  }, [isEditMode, connector, selectedConnectorDefinition.name, selectedConnectorDefinitionSpecification]);

  if (props.isLoading) {
    return <ConnectorCardLoading connectorDefinition={selectedConnectorDefinition} showHeader={!isEditMode} />;
  }

  return (
    <ConnectorForm
      canEdit={canEditConnector && (isConnectorEntitled || !connector)}
      trackDirtyChanges
      formId={props.formId}
      headerBlock={
        <FlexContainer direction="column" className={styles.header}>
          {headerBlock}
          <WarningMessage supportLevel={supportLevel} />
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
      onSubmit={(values) => onHandleSubmit(values, skipCheckConnection)}
      onCheckSection={checkSection}
      formValues={formValues}
      connectorId={connectorId}
      formHeader={!isEditMode && <ConnectorCardHeader connectorDefinition={selectedConnectorDefinition} />}
      renderFooter={({
        dirty,
        isSubmitting,
        isValid,
        emptyRequiredFields,
        formElementId,
        canEdit,
        resetConnectorForm,
        getValues,
      }) =>
        selectedConnectorDefinitionSpecification && (
          <>
            {isCloudApp &&
              selectedConnectorDefinition &&
              "sourceDefinitionId" in selectedConnectorDefinition &&
              selectedConnectorDefinition.sourceType === "database" && <AllowlistIpBanner connectorId={connectorId} />}
            {preFooterSlot}
            <Controls
              isEditMode={Boolean(isEditMode)}
              isDraftMode={Boolean(onSaveDraft)}
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
              emptyRequiredFields={emptyRequiredFields}
              dirty={dirty}
              jobId={jobInfo?.id}
              jobConfigType={jobInfo?.configType}
              jobLogs={jobInfo?.logs}
              jobFailureReason={jobInfo?.failureReason}
              onCancelClick={() => {
                resetConnectorForm();
              }}
              onSubmitWithoutCheck={
                onSaveDraft
                  ? undefined
                  : async () => {
                      const values = getValues();
                      await onHandleSubmit(values, true);
                      registerNotification({
                        id: "connector.saveWithoutCheckSuccessful",
                        type: "success",
                        text: formatMessage({ id: "connectorForm.saveWithoutCheckSuccessful" }, { name: values.name }),
                      });
                    }
              }
              onSaveDraft={
                onSaveDraft
                  ? async () => {
                      const values = getValues();
                      const connectorCardValues = {
                        ...values,
                        serviceType: Connector.id(selectedConnectorDefinition),
                      };
                      const savedDraft = await onSaveDraft(connectorCardValues, draftConnector);
                      setDraftConnector(savedDraft);
                      registerNotification({
                        id: "connector.draftSaved",
                        type: "success",
                        text: formatMessage({ id: "connectorForm.draftSaved" }, { name: values.name }),
                      });
                    }
                  : undefined
              }
              connectionTestSuccess={connectionTestSuccess}
              leftSlot={leftFooterSlot}
              formElementId={formElementId}
              canEdit={canEdit}
            />
          </>
        )
      }
    />
  );
};

const ConnectorCardLoading = ({
  connectorDefinition,
  showHeader,
}: {
  connectorDefinition: ConnectorDefinition;
  showHeader: boolean;
}) => {
  const { formatMessage } = useIntl();

  return (
    <FlexContainer direction="column" gap="xl">
      {showHeader && <ConnectorCardHeader connectorDefinition={connectorDefinition} actionsDisabled />}
      <FlexContainer direction="column" alignItems="center" gap="lg" className={styles.loaderContainer} role="status">
        <LogoAnimation title={formatMessage({ id: "ui.loading" })} />
        <SlowLoadingMessage connector={connectorDefinition.name} />
      </FlexContainer>
    </FlexContainer>
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
          <Pre>{useAirbyteCloudIpsByDataplane().join("\n")}</Pre>
        </Collapsible>
      </Box>
    </Message>
  );
};

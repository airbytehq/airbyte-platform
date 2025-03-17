import { useEffect, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Collapsible } from "components/ui/Collapsible";
import { FlexContainer } from "components/ui/Flex";
import { Icon, IconColor, IconType } from "components/ui/Icon";
import { Message } from "components/ui/Message";
import { Pre } from "components/ui/Pre";
import { ResizablePanels } from "components/ui/ResizablePanels";
import { Text } from "components/ui/Text";

import { HttpError, useCustomComponentsEnabled } from "core/api";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { links } from "core/utils/links";
import { useLocalStorage } from "core/utils/useLocalStorage";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderTestRead,
  useSelectedPageAndSlice,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import { AuxiliaryRequestsDisplay } from "./AuxiliaryRequestsDisplay";
import { LogsDisplay } from "./LogsDisplay";
import { ResultDisplay } from "./ResultDisplay";
import { StreamTestButton } from "./StreamTestButton";
import styles from "./StreamTester.module.scss";
import { useBuilderWatch } from "../useBuilderWatch";
import { useStreamTestMetadata } from "../useStreamTestMetadata";

export const StreamTester: React.FC<{
  hasTestingValuesErrors: boolean;
  setTestingValuesInputOpen: (open: boolean) => void;
}> = ({ hasTestingValuesErrors, setTestingValuesInputOpen }) => {
  const { formatMessage } = useIntl();
  const { streamNames, isResolving, resolveErrorMessage, resolveError } = useConnectorBuilderFormState();
  const {
    streamRead: {
      data: streamReadData,
      isError,
      error,
      isFetching,
      isFetchedAfterMount,
      dataUpdatedAt,
      errorUpdatedAt,
    },
    testReadLimits: { recordLimit, pageLimit, sliceLimit },
    queuedStreamRead,
    queueStreamRead,
    cancelStreamRead,
    testStreamRequestType,
  } = useConnectorBuilderTestRead();
  const [showLimitWarning, setShowLimitWarning] = useLocalStorage("connectorBuilderLimitWarning", true);
  const testStreamIndex = useBuilderWatch("testStreamIndex");
  const { selectedSlice } = useSelectedPageAndSlice();
  const globalAuxiliaryRequests = streamReadData?.auxiliary_requests;

  const streamName = streamNames[testStreamIndex];

  const analyticsService = useAnalyticsService();

  const requestErrorStatus = resolveError?.status;

  const unknownErrorMessage = formatMessage({ id: "connectorBuilder.unknownError" });
  const errorMessage = isError
    ? error instanceof HttpError
      ? error.response?.message || unknownErrorMessage
      : unknownErrorMessage
    : undefined;

  const errorExceptionStack = resolveError?.response?.exceptionStack;

  const { getStreamTestWarnings, getStreamTestMetadataStatus, getStreamHasCustomType } = useStreamTestMetadata();
  const streamTestWarnings = useMemo(() => getStreamTestWarnings(streamName), [getStreamTestWarnings, streamName]);
  const streamTestMetadataStatus = useMemo(
    () => getStreamTestMetadataStatus(streamName),
    [getStreamTestMetadataStatus, streamName]
  );
  const streamHasCustomType = getStreamHasCustomType(streamName);
  const areCustomComponentsEnabled = useCustomComponentsEnabled();
  const cantProcessCustomComponents = streamHasCustomType && !areCustomComponentsEnabled;

  const logNumByType = useMemo(
    () =>
      (streamReadData?.logs ?? []).reduce(
        (acc, log) => {
          switch (log.level) {
            case "ERROR":
            case "FATAL":
              acc.error += 1;
              break;
            case "WARN":
              acc.warning += 1;
              break;
            default:
              acc.info += 1;
              break;
          }
          return acc;
        },
        {
          info: 0,
          warning: streamTestWarnings.length,
          error: 0,
        }
      ),
    [streamReadData?.logs, streamTestWarnings.length]
  );

  const hasGlobalAuxiliaryRequests = globalAuxiliaryRequests && globalAuxiliaryRequests.length > 0;
  const hasSlices =
    streamReadData !== undefined && !isError && streamReadData.slices && streamReadData.slices.length > 0;

  const sliceAuxiliaryRequests = useMemo(() => {
    if (!hasSlices || selectedSlice === undefined) {
      return undefined;
    }
    return streamReadData.slices[selectedSlice]?.auxiliary_requests;
  }, [hasSlices, selectedSlice, streamReadData?.slices]);

  const hasSliceAuxiliaryRequests = sliceAuxiliaryRequests && sliceAuxiliaryRequests.length > 0;
  const hasAnyAuxiliaryRequests = hasGlobalAuxiliaryRequests || hasSliceAuxiliaryRequests;

  const SECONDARY_PANEL_SIZE = 0.25;
  const logsFlex = logNumByType.error > 0 || logNumByType.warning > 0 ? SECONDARY_PANEL_SIZE : 0;
  const auxiliaryRequestsFlex = hasAnyAuxiliaryRequests && !hasSlices ? SECONDARY_PANEL_SIZE : 0;

  useEffect(() => {
    // This will only be true if the data was manually refetched by the user clicking the Test button,
    // so the analytics events won't fire just from the user switching between streams, as desired
    if (isFetchedAfterMount) {
      if (errorMessage) {
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_TEST_FAILURE, {
          actionDescription: "Stream test failed",
          stream_name: streamName,
          error_message: errorMessage,
        });
      } else {
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_TEST_SUCCESS, {
          actionDescription: "Stream test succeeded",
          stream_name: streamName,
        });
      }
    }
  }, [analyticsService, errorMessage, isFetchedAfterMount, streamName, dataUpdatedAt, errorUpdatedAt]);

  return (
    <div className={styles.container}>
      {streamName === undefined && isResolving && (
        <Text size="lg" align="center">
          <FormattedMessage id="connectorBuilder.loadingStreamList" />
        </Text>
      )}

      {cantProcessCustomComponents && (
        <Message type="error" text={formatMessage({ id: "connectorBuilder.warnings.containsCustomComponent" })} />
      )}

      <StreamTestButton
        queueStreamRead={() => {
          queueStreamRead();
          analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_TEST, {
            actionDescription: "Stream test initiated",
            stream_name: streamName,
          });
        }}
        cancelStreamRead={cancelStreamRead}
        hasTestingValuesErrors={hasTestingValuesErrors}
        setTestingValuesInputOpen={setTestingValuesInputOpen}
        hasResolveErrors={Boolean(resolveErrorMessage)}
        isStreamTestQueued={queuedStreamRead}
        isStreamTestRunning={isFetching}
        className={
          !cantProcessCustomComponents && (!streamTestMetadataStatus || streamTestMetadataStatus.isStale)
            ? styles.pulsateButton
            : undefined
        }
        forceDisabled={cantProcessCustomComponents}
        requestType={testStreamRequestType}
      />

      {resolveErrorMessage !== undefined && (
        <div className={styles.listErrorDisplay}>
          <Text>
            <FormattedMessage id="connectorBuilder.couldNotValidateConnectorSpec" />
          </Text>
          <Text bold>{resolveErrorMessage}</Text>
          {errorExceptionStack && (
            <Collapsible label={formatMessage({ id: "connectorBuilder.tracebackLabel" })} className={styles.traceback}>
              <Pre longLines>{errorExceptionStack}</Pre>
            </Collapsible>
          )}
          <Text>
            {[400, 422].includes(requestErrorStatus as number) ? (
              <FormattedMessage
                id="connectorBuilder.ensureProperYaml"
                values={{
                  a: (node: React.ReactNode) => (
                    <a href={links.lowCodeYamlDescription} target="_blank" rel="noreferrer">
                      {node}
                    </a>
                  ),
                }}
              />
            ) : (
              <FormattedMessage
                id="connectorBuilder.contactSupport"
                values={{
                  a: (node: React.ReactNode) => (
                    <a href={links.supportPortal} target="_blank" rel="noreferrer">
                      {node}
                    </a>
                  ),
                }}
              />
            )}
          </Text>
        </div>
      )}
      {streamReadData && streamReadData.test_read_limit_reached && showLimitWarning && (
        <Message
          type="info"
          text={
            <FormattedMessage
              id="connectorBuilder.streamTestLimitReached"
              values={{ recordLimit, pageLimit, sliceLimit }}
            />
          }
          onClose={() => {
            setShowLimitWarning(false);
          }}
        />
      )}
      <ResizablePanels
        className={styles.resizablePanelsContainer}
        orientation="horizontal"
        panels={[
          {
            children: (
              <>
                {hasSlices && (
                  <ResultDisplay slices={streamReadData.slices} inferredSchema={streamReadData.inferred_schema} />
                )}
              </>
            ),
            minWidth: 40,
          },
          ...(errorMessage || (streamReadData?.logs && streamReadData.logs.length > 0) || streamTestWarnings.length > 0
            ? [
                {
                  children: (
                    <LogsDisplay
                      logs={streamReadData?.logs ?? []}
                      error={errorMessage}
                      testWarnings={streamTestWarnings}
                    />
                  ),
                  minWidth: 0,
                  flex: logsFlex,
                  splitter: (
                    <Splitter
                      label={formatMessage({ id: "connectorBuilder.connectorLogs" })}
                      infoNum={logNumByType.info}
                      warningNum={logNumByType.warning}
                      errorNum={logNumByType.error}
                    />
                  ),
                  className: styles.secondaryPanel,
                },
              ]
            : []),
          ...(hasAnyAuxiliaryRequests
            ? [
                {
                  children: (
                    // key causes AuxiliaryRequestsDisplay to re-mount when the selected stream or slice changes
                    <AuxiliaryRequestsDisplay
                      key={`requests_${streamName}_${selectedSlice}`}
                      globalRequests={globalAuxiliaryRequests}
                      sliceRequests={sliceAuxiliaryRequests}
                      sliceIndex={selectedSlice}
                    />
                  ),
                  minWidth: 0,
                  flex: auxiliaryRequestsFlex,
                  splitter: (
                    <Splitter
                      label={formatMessage(
                        { id: "connectorBuilder.auxiliaryRequests" },
                        { count: (globalAuxiliaryRequests?.length || 0) + (sliceAuxiliaryRequests?.length || 0) }
                      )}
                    />
                  ),
                  className: styles.secondaryPanel,
                },
              ]
            : []),
        ]}
      />
    </div>
  );
};

const Splitter = ({
  label,
  infoNum,
  warningNum,
  errorNum,
}: {
  label: string;
  infoNum?: number;
  warningNum?: number;
  errorNum?: number;
}) => (
  <FlexContainer alignItems="center" justifyContent="space-between" className={styles.splitterContainer}>
    <Text size="sm" bold>
      {label}
    </Text>
    <FlexContainer gap="md">
      {!!infoNum && <IconCount icon="infoFilled" count={infoNum} color="primary" />}
      {!!errorNum && <IconCount icon="errorFilled" count={errorNum} color="error" />}
      {!!warningNum && <IconCount icon="warningFilled" count={warningNum} color="warning" />}
    </FlexContainer>
    <FlexContainer className={styles.splitterHandleWrapper} justifyContent="center">
      <div className={styles.splitterHandle} />
    </FlexContainer>
  </FlexContainer>
);

const IconCount = ({ icon, count, color }: { icon: IconType; count: number; color: IconColor }) => (
  <FlexContainer gap="xs" alignItems="center">
    <Icon type={icon} color={color} />
    <Text size="sm" bold>
      {count}
    </Text>
  </FlexContainer>
);

import get from "lodash/get";
import isObject from "lodash/isObject";
import isString from "lodash/isString";
import { useEffect, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Collapsible } from "components/ui/Collapsible";
import { FlexContainer } from "components/ui/Flex";
import { Icon, IconColor, IconType } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { Pre } from "components/ui/Pre";
import { ResizablePanels } from "components/ui/ResizablePanels";
import { Text } from "components/ui/Text";

import { HttpError, useCustomComponentsEnabled } from "core/api";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConnectorBuilderResolve } from "core/services/connectorBuilder/ConnectorBuilderResolveContext";
import { useIsCloudApp } from "core/utils/app";
import { links } from "core/utils/links";
import { useLocalStorage } from "core/utils/useLocalStorage";
import {
  useConnectorBuilderTestRead,
  useSelectedPageAndSlice,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import { AuxiliaryRequestsDisplay } from "./AuxiliaryRequestsDisplay";
import { LogsDisplay } from "./LogsDisplay";
import { ResultDisplay } from "./ResultDisplay";
import { StreamTestButton } from "./StreamTestButton";
import styles from "./StreamTester.module.scss";
import { isStreamDynamicStream } from "../types";
import { useBuilderWatch } from "../useBuilderWatch";
import { useStreamName } from "../useStreamNames";
import { useStreamTestMetadata } from "../useStreamTestMetadata";

export const StreamTester: React.FC<{
  hasTestingValuesErrors: boolean;
  setTestingValuesInputOpen: (open: boolean) => void;
}> = ({ hasTestingValuesErrors, setTestingValuesInputOpen }) => {
  const { formatMessage } = useIntl();
  const isCloudApp = useIsCloudApp();
  const generatedStreams = useBuilderWatch("generatedStreams");
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
    generateStreams: { refetch: generateStreams, isFetching: isGeneratingStreams, error: generateStreamsError },
    queuedStreamRead,
    queueStreamRead,
    cancelStreamRead,
    testStreamRequestType,
  } = useConnectorBuilderTestRead();
  const { resolveError, isResolving, resolveErrorMessage } = useConnectorBuilderResolve();
  const [showLimitWarning, setShowLimitWarning] = useLocalStorage("connectorBuilderLimitWarning", true);
  const testStreamId = useBuilderWatch("testStreamId");
  const { selectedSlice } = useSelectedPageAndSlice();
  const globalAuxiliaryRequests = streamReadData?.auxiliary_requests;

  const streamIsDynamic = isStreamDynamicStream(testStreamId);
  const streamName = useStreamName(testStreamId) ?? "";

  const analyticsService = useAnalyticsService();

  const requestErrorStatus = resolveError?.status;

  const unknownErrorMessage = formatMessage({ id: "connectorBuilder.unknownError" });
  const errorMessage = isError
    ? error instanceof HttpError
      ? error.response?.message || unknownErrorMessage
      : unknownErrorMessage
    : generateStreamsError
    ? formatGenerateStreamsError(generateStreamsError)
    : undefined;

  const errorExceptionStack = resolveError?.response?.exceptionStack;

  const { getStreamTestWarnings, getStreamTestMetadataStatus, getStreamHasCustomType } = useStreamTestMetadata();
  const streamTestWarnings = useMemo(
    () => getStreamTestWarnings(testStreamId, true),
    [getStreamTestWarnings, testStreamId]
  );
  const streamTestMetadataStatus = useMemo(
    () => getStreamTestMetadataStatus(testStreamId),
    [getStreamTestMetadataStatus, testStreamId]
  );
  const streamHasCustomType = getStreamHasCustomType(testStreamId);
  const areCustomComponentsEnabled = useCustomComponentsEnabled();
  const cantProcessCustomComponents = streamHasCustomType && !areCustomComponentsEnabled;

  const cleanedLogs = useMemo(
    () => streamReadData?.logs?.filter((log) => !(log.level === "WARN" && log.message.includes("deprecated"))),
    [streamReadData?.logs]
  );

  const logNumByType = useMemo(
    () =>
      (cleanedLogs ?? []).reduce(
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
          error: errorMessage ? 1 : 0,
        }
      ),
    [cleanedLogs, streamTestWarnings.length, errorMessage]
  );

  const hasGeneratedStreams = generatedStreams?.[streamName]?.length > 0;

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
  const hasLogs = errorMessage || (cleanedLogs && cleanedLogs.length > 0) || streamTestWarnings.length > 0;

  const SECONDARY_PANEL_SIZE = 0.25;
  const logsFlex = hasLogs ? SECONDARY_PANEL_SIZE : 0;
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

  const streamTestButton = (
    <StreamTestButton
      variant={streamIsDynamic ? "secondary" : undefined}
      queueStreamRead={() => {
        queueStreamRead();
        if (streamIsDynamic) {
          analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.DYNAMIC_STREAM_PREVIEW_ENDPOINT, {
            actionDescription: "Dynamic stream endpoint previewed",
            dynamic_stream_name: streamName,
          });
        } else {
          analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_TEST, {
            actionDescription: "Stream test initiated",
            stream_name: streamName,
            stream_type: testStreamId.type,
            dynamic_stream_name: testStreamId.type === "generated_stream" ? testStreamId.dynamicStreamName : undefined,
          });
        }
      }}
      cancelStreamRead={cancelStreamRead}
      hasTestingValuesErrors={hasTestingValuesErrors}
      setTestingValuesInputOpen={setTestingValuesInputOpen}
      hasResolveErrors={Boolean(resolveErrorMessage)}
      isStreamTestQueued={queuedStreamRead}
      isStreamTestRunning={isFetching}
      isStreamTestStale={
        !cantProcessCustomComponents &&
        testStreamId.type !== "dynamic_stream" &&
        (!streamTestMetadataStatus || streamTestMetadataStatus.isStale)
      }
      forceDisabled={cantProcessCustomComponents}
      requestType={testStreamRequestType}
    />
  );

  return (
    <div className={styles.container}>
      {!streamName && isResolving && (
        <Text size="lg" align="center">
          <FormattedMessage id="connectorBuilder.loadingStreamList" />
        </Text>
      )}

      {cantProcessCustomComponents && (
        <Message
          type="error"
          text={
            <FormattedMessage
              id="connectorBuilder.warnings.containsCustomComponent"
              values={{
                lnk: (...lnk: React.ReactNode[]) => (
                  <ExternalLink href={links.connectorBuilderCustomComponents}>{lnk}</ExternalLink>
                ),
              }}
            />
          }
        />
      )}

      {streamIsDynamic && (
        <div className={styles.dynamicStreamButtonContainer}>
          {streamTestButton}
          <Button
            className={hasGeneratedStreams ? undefined : styles.pulsate}
            isLoading={isGeneratingStreams}
            onClick={() => {
              generateStreams();
              analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.DYNAMIC_STREAM_GENERATE, {
                actionDescription: "Dynamic streams generated",
                dynamic_stream_name: streamName,
              });
            }}
          >
            <FormattedMessage id="connectorBuilder.generateStreams" />
          </Button>
        </div>
      )}
      {!streamIsDynamic && streamTestButton}

      {resolveErrorMessage !== undefined && (
        <div className={styles.listErrorDisplay}>
          <Text>
            <FormattedMessage id="connectorBuilder.couldNotValidateConnectorSpec" />
          </Text>
          <Text bold>{resolveErrorMessage}</Text>
          {resolveError?.status === 403 && !isCloudApp && (
            <Message
              type="warning"
              text={
                <Text>
                  <FormattedMessage
                    id="connectorBuilder.fixIngress"
                    values={{
                      a: (node: React.ReactNode) => (
                        <a href={links.fixIngress1_7} target="_blank" rel="noreferrer">
                          {node}
                        </a>
                      ),
                    }}
                  />
                </Text>
              }
            />
          )}
          {errorExceptionStack && (
            <Collapsible label={formatMessage({ id: "connectorBuilder.tracebackLabel" })} className={styles.traceback}>
              <Pre longLines>
                {isString(errorExceptionStack) ? errorExceptionStack : JSON.stringify(errorExceptionStack, null, 2)}
              </Pre>
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
          ...(hasLogs
            ? [
                {
                  children: (
                    <LogsDisplay logs={cleanedLogs ?? []} error={errorMessage} testWarnings={streamTestWarnings} />
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

const formatGenerateStreamsError = (error: unknown): string | null => {
  if (isObject(error)) {
    const message = get(error, "response.message");
    if (isString(message)) {
      // The generate streams error message usually contains some text followed by a stringified JSON object.
      // That JSON object contains the actual message we want to display to the user.
      const json = extractJson(message);
      if (isObject(json) && "message" in json && isString(json.message)) {
        return json.message.replace("Please contact Airbyte Support.", "");
      }
    }
  }

  return JSON.stringify(error);
};

/**
 * Finds any stringfied JSON object in the input string by searching for
 * any text between `{` and `}`, then attempts to parse it as JSON.
 *
 * Returns null if no JSON object is found.
 */
const extractJson = (input: string): unknown | null => {
  const match = input.match(/{[\s\S]*}/);
  if (match) {
    try {
      return JSON.parse(match[0]);
    } catch {
      return null;
    }
  }
  return null;
};

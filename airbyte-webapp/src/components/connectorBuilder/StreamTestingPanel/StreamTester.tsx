import partition from "lodash/partition";
import { useEffect, useMemo } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Collapsible } from "components/ui/Collapsible";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";
import { NumberBadge } from "components/ui/NumberBadge";
import { Pre } from "components/ui/Pre";
import { ResizablePanels } from "components/ui/ResizablePanels";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";

import { HttpError } from "core/api";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { links } from "core/utils/links";
import { useLocalStorage } from "core/utils/useLocalStorage";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import { GlobalRequestsDisplay } from "./GlobalRequestsDisplay";
import { LogsDisplay } from "./LogsDisplay";
import { ResultDisplay } from "./ResultDisplay";
import { StreamTestButton } from "./StreamTestButton";
import styles from "./StreamTester.module.scss";
import { useTestWarnings } from "./useTestWarnings";
import { useBuilderWatch } from "../types";
import { useAutoImportSchema } from "../useAutoImportSchema";
import { formatJson } from "../utils";

export const StreamTester: React.FC<{
  hasTestingValuesErrors: boolean;
  setTestingValuesInputOpen: (open: boolean) => void;
}> = ({ hasTestingValuesErrors, setTestingValuesInputOpen }) => {
  const { formatMessage } = useIntl();
  const { streamNames, isResolving, resolveErrorMessage, resolveError } = useConnectorBuilderFormState();
  const {
    streamRead: {
      data: streamReadData,
      refetch: readStream,
      isError,
      error,
      isFetching,
      isFetchedAfterMount,
      dataUpdatedAt,
      errorUpdatedAt,
    },
    testReadLimits: { recordLimit, pageLimit, sliceLimit },
  } = useConnectorBuilderTestRead();
  const [showLimitWarning, setShowLimitWarning] = useLocalStorage("connectorBuilderLimitWarning", true);
  const mode = useBuilderWatch("mode");
  const testStreamIndex = useBuilderWatch("testStreamIndex");
  const { setValue } = useFormContext();
  const auxiliaryRequests = streamReadData?.auxiliary_requests;
  const autoImportSchema = useAutoImportSchema(testStreamIndex);

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

  const [errorLogs, nonErrorLogs] = useMemo(
    () =>
      streamReadData
        ? partition(streamReadData.logs, (log) => log.level === "ERROR" || log.level === "FATAL")
        : [[], []],
    [streamReadData]
  );

  const hasAuxiliaryRequests = auxiliaryRequests && auxiliaryRequests.length > 0;
  const hasRegularRequests =
    streamReadData !== undefined && !isError && streamReadData.slices && streamReadData.slices.length > 0;

  const SECONDARY_PANEL_SIZE = 0.5;
  const logsFlex = isError || errorLogs.length > 0 ? SECONDARY_PANEL_SIZE : 0;
  const auxiliaryRequestsFlex = hasAuxiliaryRequests && !hasRegularRequests ? SECONDARY_PANEL_SIZE : 0;

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

  useEffect(() => {
    if (mode === "ui" && autoImportSchema && streamReadData?.inferred_schema) {
      setValue(`formValues.streams.${testStreamIndex}.schema`, formatJson(streamReadData.inferred_schema, true), {
        shouldValidate: true,
        shouldTouch: true,
        shouldDirty: true,
      });
    }
  }, [mode, autoImportSchema, testStreamIndex, streamReadData?.inferred_schema, setValue]);

  const testDataWarnings = useTestWarnings();

  return (
    <div className={styles.container}>
      {streamName === undefined && isResolving && (
        <Text size="lg" align="center">
          <FormattedMessage id="connectorBuilder.loadingStreamList" />
        </Text>
      )}

      <StreamTestButton
        readStream={() => {
          readStream();
          analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_TEST, {
            actionDescription: "Stream test initiated",
            stream_name: streamName,
          });
        }}
        hasTestingValuesErrors={hasTestingValuesErrors}
        setTestingValuesInputOpen={setTestingValuesInputOpen}
        isResolving={isResolving}
        hasResolveErrors={Boolean(resolveErrorMessage)}
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
      {isFetching && (
        <div className={styles.fetchingSpinner}>
          <Spinner />
        </div>
      )}
      {!isFetching && streamReadData && streamReadData.test_read_limit_reached && showLimitWarning && (
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
      {!isFetching && testDataWarnings.map((warning, index) => <Message type="warning" text={warning} key={index} />)}
      {!isFetching && (streamReadData !== undefined || errorMessage !== undefined) && (
        <ResizablePanels
          className={styles.resizablePanelsContainer}
          orientation="horizontal"
          panels={[
            {
              children: (
                <>
                  {hasRegularRequests && (
                    <ResultDisplay slices={streamReadData.slices} inferredSchema={streamReadData.inferred_schema} />
                  )}
                </>
              ),
              minWidth: 40,
            },
            ...(errorMessage || (streamReadData?.logs && streamReadData.logs.length > 0)
              ? [
                  {
                    children: <LogsDisplay logs={streamReadData?.logs ?? []} error={errorMessage} />,
                    minWidth: 0,
                    flex: logsFlex,
                    splitter: (
                      <Splitter
                        label={formatMessage({ id: "connectorBuilder.connectorLogs" })}
                        num={nonErrorLogs.length}
                        errorNum={errorLogs.length}
                      />
                    ),
                    className: styles.secondaryPanel,
                  },
                ]
              : []),
            ...(hasAuxiliaryRequests
              ? [
                  {
                    children: <GlobalRequestsDisplay requests={auxiliaryRequests} />,
                    minWidth: 0,
                    flex: auxiliaryRequestsFlex,
                    splitter: (
                      <Splitter
                        label={formatMessage({ id: "connectorBuilder.auxiliaryRequests" })}
                        num={auxiliaryRequests.length}
                      />
                    ),
                    className: styles.secondaryPanel,
                  },
                ]
              : []),
          ]}
        />
      )}
    </div>
  );
};

const Splitter = ({ label, num, errorNum }: { label: string; num?: number; errorNum?: number }) => (
  <FlexContainer alignItems="center" justifyContent="space-between" className={styles.splitterContainer}>
    <Text size="sm" bold>
      {label}
    </Text>
    <FlexContainer gap="sm">
      {num !== undefined && num > 0 && <NumberBadge value={num} />}
      {errorNum !== undefined && errorNum > 0 && <NumberBadge value={errorNum} color="red" />}
    </FlexContainer>
    <FlexContainer className={styles.splitterHandleWrapper} justifyContent="center">
      <div className={styles.splitterHandle} />
    </FlexContainer>
  </FlexContainer>
);

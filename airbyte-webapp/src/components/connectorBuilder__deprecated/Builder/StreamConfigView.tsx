import classNames from "classnames";
import React, { useCallback, useEffect, useMemo } from "react";
import { get, useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { AssistButton } from "components/connectorBuilder__deprecated/Builder/Assist/AssistButton";
import GroupControls from "components/GroupControls";
import Indicator from "components/Indicator";
import { ControlLabels } from "components/LabeledControl";
import { Button } from "components/ui/Button";
import { CodeEditor } from "components/ui/CodeEditor";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { Message } from "components/ui/Message";
import { Pre } from "components/ui/Pre";
import { Text } from "components/ui/Text";

import {
  CsvDecoderType,
  GzipDecoderType,
  IterableDecoderType,
  JsonDecoderType,
  JsonlDecoderType,
  XmlDecoderType,
  ZipfileDecoderType,
} from "core/api/types/ConnectorManifest";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import {
  BuilderView,
  useConnectorBuilderFormState,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import { AuthenticationSection } from "./AuthenticationSection";
import { BuilderCard } from "./BuilderCard";
import { BuilderConfigView } from "./BuilderConfigView";
import { BuilderField } from "./BuilderField";
import { BuilderOneOf } from "./BuilderOneOf";
import { DecoderConfig } from "./DecoderConfig";
import { ErrorHandlerSection } from "./ErrorHandlerSection";
import { IncrementalSection } from "./IncrementalSection";
import { getLabelAndTooltip, getOptionsByManifest } from "./manifestHelpers";
import { PaginationSection } from "./PaginationSection";
import { ParameterizedRequestsSection } from "./ParameterizedRequestsSection";
import { ParentStreamsSection } from "./ParentStreamsSection";
import { RecordSelectorSection } from "./RecordSelectorSection";
import { RequestOptionSection } from "./RequestOptionSection";
import styles from "./StreamConfigView.module.scss";
import { TransformationSection } from "./TransformationSection";
import { UnknownFieldsSection } from "./UnknownFieldsSection";
import { SchemaConflictIndicator } from "../SchemaConflictIndicator";
import {
  BUILDER_DECODER_TYPES,
  BuilderStream,
  DECODER_CONFIGS,
  CreationRequesterPathFn,
  DownloadRequesterPathFn,
  PollingRequesterPathFn,
  StreamPathFn,
  isEmptyOrDefault,
  DEFAULT_BUILDER_STREAM_VALUES,
  DEFAULT_BUILDER_ASYNC_STREAM_VALUES,
  BuilderPollingTimeout,
  StreamId,
  getStreamFieldPath,
  AnyDeclarativeStreamPathFn,
  BuilderDecoderConfig,
} from "../types";
import { useAutoImportSchema } from "../useAutoImportSchema";
import { useBuilderErrors } from "../useBuilderErrors";
import { useBuilderWatch } from "../useBuilderWatch";
import { formatJson } from "../utils";

interface StreamConfigViewProps {
  streamId: StreamId;
  scrollToTop: () => void;
}

export const StreamConfigView: React.FC<StreamConfigViewProps> = React.memo(({ streamId, scrollToTop }) => {
  const analyticsService = useAnalyticsService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const streams = useBuilderWatch("formValues.streams");
  const dynamicStreams = useBuilderWatch("formValues.dynamicStreams");
  const generatedStreams = useBuilderWatch("formValues.generatedStreams");

  const { setValue } = useFormContext();

  const currentStream = useMemo(() => {
    if (streamId.type === "stream") {
      return streams[streamId.index];
    } else if (streamId.type === "generated_stream") {
      return generatedStreams[streamId.dynamicStreamName][streamId.index];
    }
    return dynamicStreams[streamId.index].streamTemplate;
  }, [streamId, streams, dynamicStreams, generatedStreams]);

  const otherStreamsWithSameRequestTypeExist = useMemo(() => {
    if (streamId.type === "stream") {
      return streams.some(
        (stream, index) => index !== streamId.index && stream.requestType === currentStream.requestType
      );
    }
    return false;
  }, [streams, streamId, currentStream.requestType]);

  const handleDelete = () => {
    openConfirmationModal({
      text: "connectorBuilder.deleteStreamModal.text",
      title: "connectorBuilder.deleteStreamModal.title",
      submitButtonText: "connectorBuilder.deleteStreamModal.submitButton",
      onSubmit: () => {
        if (streamId.type !== "stream") {
          return;
        }
        const updatedStreams: BuilderStream[] = streams.filter((_, index) => index !== streamId.index);
        const streamToSelect = streamId.index >= updatedStreams.length ? updatedStreams.length - 1 : streamId.index;
        const viewToSelect: BuilderView =
          updatedStreams.length === 0 ? { type: "global" } : { type: "stream", index: streamToSelect };
        setValue("formValues.streams", updatedStreams);
        setValue("view", viewToSelect);
        closeConfirmationModal();
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_DELETE, {
          actionDescription: "Stream deleted",
          stream_id: streamId.index,
          stream_name: currentStream.name,
        });
      },
    });
  };

  return (
    <BuilderConfigView
      className={classNames(styles.relative, { [styles.multiStreams]: otherStreamsWithSameRequestTypeExist })}
    >
      {streamId.type === "stream" && (
        <FlexContainer justifyContent="space-between" className={styles.relative} alignItems="center">
          <BuilderField
            type="string"
            path={getStreamFieldPath(streamId, "name")}
            containerClassName={styles.streamNameInput}
          />

          <Button variant="danger" onClick={handleDelete}>
            <FormattedMessage id="connectorBuilder.deleteStreamModal.title" />
          </Button>
        </FlexContainer>
      )}
      {streamId.type === "generated_stream" && (
        <Message
          type="info"
          text={
            <FormattedMessage
              id="connectorBuilder.generatedStream.readonlyDescription"
              values={{
                lnk: () => (
                  <Button
                    variant="link"
                    onClick={() => {
                      setValue("view", {
                        type: "dynamic_stream",
                        index: dynamicStreams.findIndex(
                          (stream) => stream.dynamicStreamName === streamId.dynamicStreamName
                        ),
                      });
                    }}
                  >
                    <Text bold>
                      {
                        dynamicStreams.find((stream) => stream.dynamicStreamName === streamId.dynamicStreamName)
                          ?.dynamicStreamName
                      }
                    </Text>
                  </Button>
                ),
              }}
            />
          }
        />
      )}
      {currentStream.requestType === "sync" ? (
        <SynchronousStream streamId={streamId} scrollToTop={scrollToTop} />
      ) : (
        <AsynchronousStream streamId={streamId} scrollToTop={scrollToTop} />
      )}
    </BuilderConfigView>
  );
});

StreamConfigView.displayName = "StreamConfigView";

interface SynchronousStreamProps {
  streamId: StreamId;
  scrollToTop: () => void;
}
const SynchronousStream: React.FC<SynchronousStreamProps> = ({ streamId, scrollToTop }) => {
  const { formatMessage } = useIntl();
  const { permission } = useConnectorBuilderFormState();
  const streamTab = useBuilderWatch("streamTab");
  const dynamicStreams = useBuilderWatch("formValues.dynamicStreams");
  const { setValue } = useFormContext();
  const { hasErrors } = useBuilderErrors();

  const baseUrl = useBuilderWatch("formValues.global.urlBase");
  const streamFieldPath = ((fieldPath: string) =>
    getStreamFieldPath(streamId, fieldPath)) as AnyDeclarativeStreamPathFn;
  const selectedDecoder = useBuilderWatch(streamFieldPath("decoder")) as BuilderDecoderConfig;

  useEffect(() => {
    if (streamTab !== "requester" && streamTab !== "schema") {
      setValue("streamTab", "requester");
    }
  }, [setValue, streamTab]);

  return (
    <>
      <FlexContainer className={styles.sticky} justifyContent="space-between" alignItems="center">
        <FlexContainer>
          <StreamTab
            data-testid="tag-tab-stream-configuration"
            label={formatMessage({ id: "connectorBuilder.streamConfiguration" })}
            isSelected={streamTab === "requester"}
            onSelect={() => {
              setValue("streamTab", "requester");
              scrollToTop();
            }}
            showErrorIndicator={hasErrors([{ type: "stream", index: streamId.index }], "requester")}
          />
          <SchemaTab
            streamId={streamId}
            isSelected={streamTab === "schema"}
            onSelect={() => {
              setValue("streamTab", "schema");
              scrollToTop();
            }}
          />
        </FlexContainer>
        {streamId.type === "stream" && (
          <RequestTypeSelector streamId={streamId} streamFieldPath={streamFieldPath} selectedValue="sync" />
        )}
      </FlexContainer>
      {streamTab === "requester" ? (
        <fieldset
          disabled={permission === "readOnly" || streamId.type === "generated_stream"}
          className={styles.fieldset}
        >
          {streamId.type === "dynamic_stream" && (
            <BuilderCard>
              <BuilderField
                type="jinja"
                path={streamFieldPath("name")}
                manifestPath="DeclarativeStream.properties.name"
              />
            </BuilderCard>
          )}
          <BuilderCard>
            <BuilderField
              type="jinja"
              path={streamFieldPath("urlPath")}
              manifestPath="HttpRequester.properties.path"
              preview={baseUrl ? (value) => `${baseUrl}${value}` : undefined}
              labelAction={<AssistButton assistKey="metadata" streamId={streamId} />}
            />
            <BuilderField
              type="enum"
              path={streamFieldPath("httpMethod")}
              options={getOptionsByManifest("HttpRequester.properties.http_method")}
              manifestPath="HttpRequester.properties.http_method"
            />
            <BuilderField
              type="enum"
              label={formatMessage({ id: "connectorBuilder.decoder.label" })}
              tooltip={formatMessage({ id: "connectorBuilder.decoder.tooltip" })}
              path={streamFieldPath("decoder.type")}
              options={[...BUILDER_DECODER_TYPES]}
              manifestPath="SimpleRetriever.properties.decoder"
              manifestOptionPaths={[
                JsonDecoderType.JsonDecoder,
                XmlDecoderType.XmlDecoder,
                JsonlDecoderType.JsonlDecoder,
                IterableDecoderType.IterableDecoder,
                CsvDecoderType.CsvDecoder,
                GzipDecoderType.GzipDecoder,
                ZipfileDecoderType.ZipfileDecoder,
              ]}
            />
            {selectedDecoder.type && DECODER_CONFIGS[selectedDecoder.type] && (
              <DecoderConfig
                decoderType={selectedDecoder.type}
                decoderFieldPath={(fieldPath: string) => `${streamFieldPath("decoder")}.${fieldPath}`}
              />
            )}
            <BuilderField
              type="array"
              path={streamFieldPath("primaryKey")}
              label={formatMessage({ id: "connectorBuilder.streamConfigView.primaryKey.label" })}
              tooltip={formatMessage({ id: "connectorBuilder.streamConfigView.primaryKey.tooltip" })}
              directionalStyle={false}
              optional
            />
          </BuilderCard>
          <RecordSelectorSection streamFieldPath={streamFieldPath} streamId={streamId} />
          <RequestOptionSection inline={false} basePath={streamFieldPath("requestOptions")} streamId={streamId} />
          <PaginationSection streamFieldPath={streamFieldPath} streamId={streamId} />
          <IncrementalSection streamFieldPath={streamFieldPath} streamId={streamId} />
          {streamId.type === "stream" && (
            <ParentStreamsSection
              streamFieldPath={streamFieldPath as StreamPathFn}
              currentStreamIndex={streamId.index}
            />
          )}
          <ParameterizedRequestsSection streamFieldPath={streamFieldPath} streamId={streamId} />
          <ErrorHandlerSection inline={false} basePath={streamFieldPath("errorHandler")} streamId={streamId} />
          <TransformationSection streamFieldPath={streamFieldPath} streamId={streamId} />
          <UnknownFieldsSection streamFieldPath={streamFieldPath} />
        </fieldset>
      ) : streamTab === "schema" ? (
        <BuilderCard className={styles.schemaEditor}>
          <SchemaEditor
            streamFieldPath={
              streamId.type === "generated_stream"
                ? (((field: string) =>
                    getStreamFieldPath(
                      {
                        type: "dynamic_stream",
                        index: dynamicStreams.findIndex(
                          (stream) => stream.dynamicStreamName === streamId.dynamicStreamName
                        ),
                      },
                      field
                    )) as AnyDeclarativeStreamPathFn)
                : streamFieldPath
            }
          />
        </BuilderCard>
      ) : null}
    </>
  );
};

interface AsynchronousStreamProps {
  streamId: StreamId;
  scrollToTop: () => void;
}
const AsynchronousStream: React.FC<AsynchronousStreamProps> = ({ streamId, scrollToTop }) => {
  const { formatMessage } = useIntl();
  const streamTab = useBuilderWatch("streamTab");
  const { setValue } = useFormContext();
  const { hasErrors } = useBuilderErrors();

  useEffect(() => {
    if (streamTab !== "requester" && streamTab !== "polling" && streamTab !== "download" && streamTab !== "schema") {
      setValue("streamTab", "requester");
    }
  }, [setValue, streamTab]);

  const streamFieldPath = useMemo(() => {
    return ((fieldPath: string) => {
      return `${
        streamId.type === "generated_stream"
          ? (`formValues.generatedStreams.${streamId.dynamicStreamName}` as const)
          : ("formValues.streams" as const)
      }.${streamId.index}.${fieldPath}`;
    }) as AnyDeclarativeStreamPathFn;
  }, [streamId]);

  const creationRequesterPath: CreationRequesterPathFn = useCallback(
    <T extends string>(fieldPath: T) => `formValues.streams.${streamId.index}.creationRequester.${fieldPath}` as const,
    [streamId.index]
  );
  const pollingRequesterPath: PollingRequesterPathFn = useCallback(
    <T extends string>(fieldPath: T) => `formValues.streams.${streamId.index}.pollingRequester.${fieldPath}` as const,
    [streamId.index]
  );
  const downloadRequesterPath: DownloadRequesterPathFn = useCallback(
    <T extends string>(fieldPath: T) => `formValues.streams.${streamId.index}.downloadRequester.${fieldPath}` as const,
    [streamId.index]
  );

  const selectedCreationDecoder = useBuilderWatch(creationRequesterPath("decoder"));
  const selectedDownloadDecoder = useBuilderWatch(downloadRequesterPath("decoder"));

  const tabContentKey = useMemo(() => `${streamId.index}-${streamTab}`, [streamId.index, streamTab]);

  if (streamId.type !== "stream") {
    return null;
  }
  return (
    <>
      <FlexContainer className={styles.sticky} justifyContent="space-between" alignItems="center">
        <FlexContainer>
          <StreamTab
            data-testid="tag-tab-async-stream-creation"
            label={formatMessage({ id: "connectorBuilder.asyncStream.creation" })}
            isSelected={streamTab === "requester"}
            onSelect={() => {
              setValue("streamTab", "requester");
              scrollToTop();
            }}
            showErrorIndicator={hasErrors([{ type: "stream", index: streamId.index }], "requester")}
          />
          <StreamTab
            data-testid="tag-tab-async-stream-polling"
            label={formatMessage({ id: "connectorBuilder.asyncStream.polling" })}
            isSelected={streamTab === "polling"}
            onSelect={() => {
              setValue("streamTab", "polling");
              scrollToTop();
            }}
            showErrorIndicator={hasErrors([{ type: "stream", index: streamId.index }], "polling")}
          />
          <StreamTab
            data-testid="tag-tab-async-stream-download"
            label={formatMessage({ id: "connectorBuilder.asyncStream.download" })}
            isSelected={streamTab === "download"}
            onSelect={() => {
              setValue("streamTab", "download");
              scrollToTop();
            }}
            showErrorIndicator={hasErrors([{ type: "stream", index: streamId.index }], "download")}
          />
          <SchemaTab
            streamId={streamId}
            isSelected={streamTab === "schema"}
            onSelect={() => {
              setValue("streamTab", "schema");
              scrollToTop();
            }}
          />
        </FlexContainer>
        <RequestTypeSelector streamId={streamId} streamFieldPath={streamFieldPath} selectedValue="async" />
      </FlexContainer>
      {streamTab === "requester" ? (
        <FlexContainer key={tabContentKey} direction="column">
          <BuilderCard>
            <BuilderField
              type="jinja"
              path={creationRequesterPath("url")}
              label={formatMessage({ id: "connectorBuilder.asyncStream.url.label" })}
              tooltip={formatMessage({ id: "connectorBuilder.asyncStream.url.tooltip" })}
            />
            <BuilderField
              type="enum"
              path={creationRequesterPath("httpMethod")}
              options={getOptionsByManifest("HttpRequester.properties.http_method")}
              manifestPath="HttpRequester.properties.http_method"
            />
            <BuilderField
              type="enum"
              path={creationRequesterPath("decoder.type")}
              label={formatMessage({ id: "connectorBuilder.decoder.label" })}
              tooltip={formatMessage({ id: "connectorBuilder.decoder.tooltip" })}
              options={[...BUILDER_DECODER_TYPES]}
              manifestPath="SimpleRetriever.properties.decoder"
              manifestOptionPaths={[
                JsonDecoderType.JsonDecoder,
                XmlDecoderType.XmlDecoder,
                JsonlDecoderType.JsonlDecoder,
                IterableDecoderType.IterableDecoder,
                CsvDecoderType.CsvDecoder,
              ]}
            />
            {selectedCreationDecoder.type && DECODER_CONFIGS[selectedCreationDecoder.type] && (
              <DecoderConfig
                decoderType={selectedCreationDecoder.type}
                decoderFieldPath={(fieldPath: string) => `${creationRequesterPath("decoder")}.${fieldPath}`}
              />
            )}
          </BuilderCard>
          <AuthenticationSection authPath={creationRequesterPath("authenticator")} />
          <RequestOptionSection inline={false} basePath={creationRequesterPath("requestOptions")} streamId={streamId} />
          <IncrementalSection streamFieldPath={creationRequesterPath} streamId={streamId} />
          {streamId.type === "stream" && (
            <ParentStreamsSection streamFieldPath={creationRequesterPath} currentStreamIndex={streamId.index} />
          )}
          <ParameterizedRequestsSection streamFieldPath={creationRequesterPath} streamId={streamId} />
          <ErrorHandlerSection inline={false} basePath={creationRequesterPath("errorHandler")} streamId={streamId} />
        </FlexContainer>
      ) : streamTab === "polling" ? (
        <FlexContainer key={tabContentKey} direction="column">
          <BuilderCard>
            <BuilderField
              type="jinja"
              path={pollingRequesterPath("url")}
              label={formatMessage({ id: "connectorBuilder.asyncStream.url.label" })}
              tooltip={formatMessage({ id: "connectorBuilder.asyncStream.url.tooltip" })}
            />
            <BuilderField
              type="enum"
              path={pollingRequesterPath("httpMethod")}
              options={getOptionsByManifest("HttpRequester.properties.http_method")}
              manifestPath="HttpRequester.properties.http_method"
            />
          </BuilderCard>
          <AuthenticationSection authPath={pollingRequesterPath("authenticator")} />
          <BuilderCard>
            <BuilderOneOf<BuilderPollingTimeout>
              path={pollingRequesterPath("pollingTimeout")}
              label={formatMessage({ id: "connectorBuilder.asyncStream.polling.timeout.label" })}
              manifestPath="AsyncRetriever.properties.polling_job_timeout"
              options={[
                {
                  label: formatMessage({ id: "connectorBuilder.asyncStream.polling.timeout.number" }),
                  default: {
                    type: "number",
                    value: 15,
                  },
                  children: (
                    <BuilderField
                      type="integer"
                      path={pollingRequesterPath("pollingTimeout.value")}
                      label={formatMessage({ id: "connectorBuilder.asyncStream.polling.timeout.number.label" })}
                      tooltip={formatMessage({ id: "connectorBuilder.asyncStream.polling.timeout.number.tooltip" })}
                      step={1}
                      min={1}
                    />
                  ),
                },
                {
                  label: formatMessage({ id: "connectorBuilder.asyncStream.polling.timeout.custom" }),
                  default: {
                    type: "custom",
                    value: "{{ }}",
                  },
                  children: (
                    <BuilderField
                      type="jinja"
                      path={pollingRequesterPath("pollingTimeout.value")}
                      label={formatMessage({ id: "connectorBuilder.asyncStream.polling.timeout.custom.label" })}
                      tooltip={formatMessage({ id: "connectorBuilder.asyncStream.polling.timeout.custom.tooltip" })}
                      pattern={formatMessage({ id: "connectorBuilder.asyncStream.polling.timeout.custom.pattern" })}
                    />
                  ),
                },
              ]}
            />
            <GroupControls
              label={
                <ControlLabels
                  label={formatMessage({ id: "connectorBuilder.asyncStream.polling.statusExtractor.label" })}
                  infoTooltipContent={
                    getLabelAndTooltip(
                      formatMessage({ id: "connectorBuilder.asyncStream.polling.statusExtractor.label" }),
                      undefined,
                      "AsyncRetriever.properties.status_extractor"
                    ).tooltip
                  }
                />
              }
            >
              <BuilderField
                type="array"
                path={pollingRequesterPath("statusExtractor.field_path")}
                manifestPath="DpathExtractor.properties.field_path"
              />
            </GroupControls>
            <GroupControls
              label={
                <ControlLabels
                  label={formatMessage({ id: "connectorBuilder.asyncStream.polling.statusMapping.label" })}
                  infoTooltipContent={
                    getLabelAndTooltip(
                      formatMessage({ id: "connectorBuilder.asyncStream.polling.statusMapping.label" }),
                      formatMessage({
                        id: "connectorBuilder.asyncStream.polling.statusMapping.tooltip",
                      }),
                      undefined
                    ).tooltip
                  }
                />
              }
            >
              <BuilderField
                type="array"
                directionalStyle={false}
                path={pollingRequesterPath("statusMapping.completed")}
                label={formatMessage({ id: "connectorBuilder.asyncStream.polling.statusMapping.completed" })}
              />
              <BuilderField
                type="array"
                directionalStyle={false}
                path={pollingRequesterPath("statusMapping.failed")}
                label={formatMessage({ id: "connectorBuilder.asyncStream.polling.statusMapping.failed" })}
              />
              <BuilderField
                type="array"
                directionalStyle={false}
                path={pollingRequesterPath("statusMapping.running")}
                label={formatMessage({ id: "connectorBuilder.asyncStream.polling.statusMapping.running" })}
              />
              <BuilderField
                type="array"
                directionalStyle={false}
                path={pollingRequesterPath("statusMapping.timeout")}
                label={formatMessage({ id: "connectorBuilder.asyncStream.polling.statusMapping.timeout" })}
              />
            </GroupControls>
            <GroupControls
              label={
                <ControlLabels
                  label={formatMessage({ id: "connectorBuilder.asyncStream.polling.downloadTargetExtractor.label" })}
                  infoTooltipContent={
                    getLabelAndTooltip(
                      formatMessage({ id: "connectorBuilder.asyncStream.polling.downloadTargetExtractor.label" }),
                      undefined,
                      "AsyncRetriever.properties.download_target_extractor"
                    ).tooltip
                  }
                />
              }
            >
              <BuilderField
                type="array"
                path={pollingRequesterPath("downloadTargetExtractor.field_path")}
                manifestPath="DpathExtractor.properties.field_path"
              />
            </GroupControls>
          </BuilderCard>
          <RequestOptionSection inline={false} basePath={pollingRequesterPath("requestOptions")} streamId={streamId} />
          <ErrorHandlerSection inline={false} basePath={pollingRequesterPath("errorHandler")} streamId={streamId} />
        </FlexContainer>
      ) : streamTab === "download" ? (
        <FlexContainer key={tabContentKey} direction="column">
          <BuilderCard>
            <BuilderField
              type="jinja"
              path={downloadRequesterPath("url")}
              label={formatMessage({ id: "connectorBuilder.asyncStream.url.label" })}
              tooltip={formatMessage({ id: "connectorBuilder.asyncStream.url.tooltip" })}
            />
            <BuilderField
              type="enum"
              path={downloadRequesterPath("httpMethod")}
              options={getOptionsByManifest("HttpRequester.properties.http_method")}
              manifestPath="HttpRequester.properties.http_method"
            />
            <BuilderField
              type="enum"
              path={downloadRequesterPath("decoder.type")}
              label={formatMessage({ id: "connectorBuilder.decoder.label" })}
              tooltip={formatMessage({ id: "connectorBuilder.decoder.tooltip" })}
              options={[...BUILDER_DECODER_TYPES]}
              manifestPath="SimpleRetriever.properties.decoder"
              manifestOptionPaths={[
                JsonDecoderType.JsonDecoder,
                XmlDecoderType.XmlDecoder,
                JsonlDecoderType.JsonlDecoder,
                IterableDecoderType.IterableDecoder,
                CsvDecoderType.CsvDecoder,
              ]}
            />
            {selectedDownloadDecoder.type && DECODER_CONFIGS[selectedDownloadDecoder.type] && (
              <DecoderConfig
                decoderType={selectedDownloadDecoder.type}
                decoderFieldPath={(fieldPath: string) => `${downloadRequesterPath("decoder")}.${fieldPath}`}
              />
            )}
            <GroupControls
              label={
                <ControlLabels
                  label={formatMessage({ id: "connectorBuilder.asyncStream.download.extractor.label" })}
                  infoTooltipContent={
                    getLabelAndTooltip(
                      formatMessage({ id: "connectorBuilder.asyncStream.download.extractor.label" }),
                      undefined,
                      "AsyncRetriever.properties.download_extractor"
                    ).tooltip
                  }
                />
              }
            >
              <BuilderField
                type="array"
                path={downloadRequesterPath("downloadExtractor.field_path")}
                manifestPath="DpathExtractor.properties.field_path"
                optional
              />
            </GroupControls>
            <BuilderField
              type="array"
              path={downloadRequesterPath("primaryKey")}
              label={formatMessage({ id: "connectorBuilder.streamConfigView.primaryKey.label" })}
              tooltip={formatMessage({ id: "connectorBuilder.streamConfigView.primaryKey.tooltip" })}
              directionalStyle={false}
              optional
            />
          </BuilderCard>
          <AuthenticationSection authPath={downloadRequesterPath("authenticator")} />
          <RecordSelectorSection streamFieldPath={downloadRequesterPath} streamId={streamId} />
          <RequestOptionSection inline={false} basePath={downloadRequesterPath("requestOptions")} streamId={streamId} />
          <PaginationSection streamFieldPath={downloadRequesterPath} streamId={streamId} />
          <TransformationSection streamFieldPath={downloadRequesterPath} streamId={streamId} />
          <ErrorHandlerSection inline={false} basePath={downloadRequesterPath("errorHandler")} streamId={streamId} />
        </FlexContainer>
      ) : streamTab === "schema" ? (
        <BuilderCard className={styles.schemaEditor}>
          <SchemaEditor streamFieldPath={streamFieldPath} />
        </BuilderCard>
      ) : null}
    </>
  );
};

const RequestTypeSelector = ({
  streamId,
  streamFieldPath,
  selectedValue,
}: {
  streamId: StreamId;
  streamFieldPath: AnyDeclarativeStreamPathFn;
  selectedValue: BuilderStream["requestType"];
}) => {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { setValue } = useFormContext();
  const id = useBuilderWatch(streamFieldPath("id"));
  const name = useBuilderWatch(streamFieldPath("name"));

  const handleSelect = useCallback(
    (newValue: BuilderStream["requestType"]) => {
      if (newValue === selectedValue) {
        return;
      }

      openConfirmationModal({
        title:
          newValue === "sync"
            ? "connectorBuilder.requestType.confirm.title.sync"
            : "connectorBuilder.requestType.confirm.title.async",
        text: "connectorBuilder.requestType.confirm.text",
        submitButtonText:
          newValue === "sync"
            ? "connectorBuilder.requestType.confirm.submit.sync"
            : "connectorBuilder.requestType.confirm.submit.async",
        submitButtonVariant: "primary",
        cancelButtonText: "connectorBuilder.requestType.confirm.cancel",
        onSubmit: () => {
          if (newValue === "sync") {
            setValue(`formValues.streams.${streamId.index}`, {
              ...DEFAULT_BUILDER_STREAM_VALUES,
              id,
              name,
            });
          } else if (newValue === "async") {
            setValue(`formValues.streams.${streamId.index}`, {
              ...DEFAULT_BUILDER_ASYNC_STREAM_VALUES,
              id,
              name,
            });
          }
          closeConfirmationModal();
        },
      });
    },
    [selectedValue, openConfirmationModal, closeConfirmationModal, setValue, streamId.index, id, name]
  );

  return (
    <FlexContainer alignItems="center">
      <Text color="grey500" align="right">
        <FormattedMessage id="connectorBuilder.requestType" />:
      </Text>
      <ListBox<BuilderStream["requestType"]>
        selectedValue={selectedValue}
        onSelect={handleSelect}
        options={[
          {
            label: <FormattedMessage id="connectorBuilder.requestType.sync" />,
            value: "sync",
          },
          {
            label: <FormattedMessage id="connectorBuilder.requestType.async" />,
            value: "async",
          },
        ]}
        placement="bottom-end"
        adaptiveWidth={false}
        flip={false}
        buttonClassName={styles.requestTypeButton}
      />
    </FlexContainer>
  );
};

const StreamTab = ({
  isSelected,
  label,
  onSelect,
  showErrorIndicator,
  showSchemaConflictIndicator,
  schemaErrors,
  "data-testid": testId,
}: {
  isSelected: boolean;
  label: string;
  onSelect: () => void;
  showErrorIndicator?: boolean;
  showSchemaConflictIndicator?: boolean;
  schemaErrors?: string[];
  "data-testid": string;
}) => (
  <button
    data-testid={testId}
    type="button"
    className={classNames(styles.tab, { [styles.selectedTab]: isSelected })}
    onClick={onSelect}
  >
    <Text color={isSelected ? "darkBlue" : "grey400"}>{label}</Text>
    {showErrorIndicator && <Indicator />}
    {showSchemaConflictIndicator && <SchemaConflictIndicator errors={schemaErrors} />}
  </button>
);

const SchemaTab = ({
  streamId,
  isSelected,
  onSelect,
}: {
  streamId: StreamId;
  isSelected: boolean;
  onSelect: () => void;
}) => {
  const { formatMessage } = useIntl();
  const {
    schemaWarnings: { incompatibleSchemaErrors, schemaDifferences },
  } = useConnectorBuilderTestRead();
  const { hasErrors } = useBuilderErrors();
  const autoImportSchema = useAutoImportSchema(streamId);

  return (
    <StreamTab
      data-testid="tag-tab-stream-schema"
      label={formatMessage({ id: "connectorBuilder.streamSchema" })}
      isSelected={isSelected}
      onSelect={() => onSelect()}
      showErrorIndicator={hasErrors([{ type: "stream", index: streamId.index }], "schema")}
      showSchemaConflictIndicator={schemaDifferences && !autoImportSchema}
      schemaErrors={incompatibleSchemaErrors}
    />
  );
};

const SchemaEditor = ({ streamFieldPath }: { streamFieldPath: AnyDeclarativeStreamPathFn }) => {
  const { formatMessage } = useIntl();
  const analyticsService = useAnalyticsService();
  const { permission, streamIdToStreamRepresentation } = useConnectorBuilderFormState();
  const autoImportSchemaFieldPath = streamFieldPath("autoImportSchema");
  const autoImportSchema = useBuilderWatch(autoImportSchemaFieldPath);
  const schemaFieldPath = streamFieldPath("schema");
  const schema = useBuilderWatch(schemaFieldPath) as BuilderStream["schema"];
  const testStreamId = useBuilderWatch("testStreamId");
  const { setValue } = useFormContext();
  const { errors } = useFormState({ name: schemaFieldPath });
  const error = get(errors, schemaFieldPath);
  const { streamRead } = useConnectorBuilderTestRead();

  const showImportButton = !autoImportSchema && isEmptyOrDefault(schema) && streamRead.data?.inferred_schema;
  const formattedSchema = useMemo(() => {
    try {
      return schema ? formatJson(JSON.parse(schema)) : undefined;
    } catch (e) {
      return undefined;
    }
  }, [schema]);

  return (
    <>
      <BuilderField
        label={formatMessage({ id: "connectorBuilder.autoImportSchema.label" })}
        path={autoImportSchemaFieldPath}
        type="boolean"
        tooltip={<FormattedMessage id="connectorBuilder.autoImportSchema.tooltip" values={{ br: () => <br /> }} />}
        disabled={
          (error && !streamRead.data?.inferred_schema) ||
          permission === "readOnly" ||
          testStreamId.type === "generated_stream"
        }
        disabledTooltip={
          permission === "readOnly"
            ? undefined
            : formatMessage({ id: "connectorBuilder.autoImportSchema.disabledTooltip" })
        }
      />
      {showImportButton && (
        <Button
          full
          type="button"
          variant="secondary"
          onClick={() => {
            const formattedJson = formatJson(streamRead.data?.inferred_schema, true);
            setValue(schemaFieldPath, formattedJson);
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.OVERWRITE_SCHEMA, {
              actionDescription: "Declared schema overwritten by detected schema",
              ...streamIdToStreamRepresentation(testStreamId),
            });
          }}
        >
          <FormattedMessage id="connectorBuilder.useSchemaButton" />
        </Button>
      )}
      {autoImportSchema ? (
        <div className={styles.autoSchemaContainer}>
          <Pre>{formattedSchema}</Pre>
        </div>
      ) : (
        <div className={styles.editorContainer}>
          <CodeEditor
            readOnly={permission === "readOnly" || testStreamId.type === "generated_stream"}
            key={schemaFieldPath}
            value={schema || ""}
            language="json"
            onChange={(val: string | undefined) => {
              setValue(schemaFieldPath, val, {
                shouldValidate: true,
                shouldDirty: true,
                shouldTouch: true,
              });
            }}
          />
        </div>
      )}
      {error && (
        <Text className={styles.errorMessage}>
          <FormattedMessage id={error.message} />
        </Text>
      )}
    </>
  );
};

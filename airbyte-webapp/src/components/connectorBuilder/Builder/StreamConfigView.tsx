import classNames from "classnames";
import React, { useCallback, useEffect, useMemo } from "react";
import { get, useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { AssistButton } from "components/connectorBuilder/Builder/Assist/AssistButton";
import GroupControls from "components/GroupControls";
import Indicator from "components/Indicator";
import { ControlLabels } from "components/LabeledControl";
import { Button } from "components/ui/Button";
import { CodeEditor } from "components/ui/CodeEditor";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
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
} from "services/connectorBuilder/ConnectorBuilderStateService";

import { AuthenticationSection } from "./AuthenticationSection";
import { BuilderCard } from "./BuilderCard";
import { BuilderConfigView } from "./BuilderConfigView";
import { BuilderField } from "./BuilderField";
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
} from "../types";
import { useAutoImportSchema } from "../useAutoImportSchema";
import { useBuilderErrors } from "../useBuilderErrors";
import { useBuilderWatch } from "../useBuilderWatch";
import { formatJson } from "../utils";

interface StreamConfigViewProps {
  streamNum: number;
  scrollToTop: () => void;
}

export const StreamConfigView: React.FC<StreamConfigViewProps> = React.memo(({ streamNum, scrollToTop }) => {
  const analyticsService = useAnalyticsService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const streams = useBuilderWatch("formValues.streams");
  const { setValue } = useFormContext();
  const currentStream = useMemo(() => streams[streamNum], [streams, streamNum]);
  const otherStreamsWithSameRequestTypeExist = useMemo(() => {
    return streams.some((stream, index) => index !== streamNum && stream.requestType === currentStream.requestType);
  }, [streams, streamNum, currentStream.requestType]);

  const handleDelete = () => {
    openConfirmationModal({
      text: "connectorBuilder.deleteStreamModal.text",
      title: "connectorBuilder.deleteStreamModal.title",
      submitButtonText: "connectorBuilder.deleteStreamModal.submitButton",
      onSubmit: () => {
        const updatedStreams: BuilderStream[] = streams.filter((_, index) => index !== streamNum);
        const streamToSelect = streamNum >= updatedStreams.length ? updatedStreams.length - 1 : streamNum;
        const viewToSelect: BuilderView = updatedStreams.length === 0 ? "global" : streamToSelect;
        setValue("formValues.streams", updatedStreams);
        setValue("view", viewToSelect);
        closeConfirmationModal();
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_DELETE, {
          actionDescription: "New stream created from the Add Stream button",
          stream_id: streams[streamNum].id,
          stream_name: streams[streamNum].name,
        });
      },
    });
  };

  return (
    <BuilderConfigView
      className={classNames(styles.relative, { [styles.multiStreams]: otherStreamsWithSameRequestTypeExist })}
    >
      <FlexContainer justifyContent="space-between" className={styles.relative} alignItems="center">
        <BuilderField
          type="string"
          path={`formValues.streams.${streamNum}.name`}
          containerClassName={styles.streamNameInput}
        />
        <Button variant="danger" onClick={handleDelete}>
          <FormattedMessage id="connectorBuilder.deleteStreamModal.title" />
        </Button>
      </FlexContainer>
      {currentStream.requestType === "sync" ? (
        <SynchronousStream streamNum={streamNum} scrollToTop={scrollToTop} />
      ) : (
        <AsynchronousStream streamNum={streamNum} scrollToTop={scrollToTop} />
      )}
    </BuilderConfigView>
  );
});

StreamConfigView.displayName = "StreamConfigView";

interface SynchronousStreamProps {
  streamNum: number;
  scrollToTop: () => void;
}
const SynchronousStream: React.FC<SynchronousStreamProps> = ({ streamNum, scrollToTop }) => {
  const { formatMessage } = useIntl();
  const { permission } = useConnectorBuilderFormState();
  const streamTab = useBuilderWatch("streamTab");
  const { setValue } = useFormContext();
  const { hasErrors } = useBuilderErrors();

  const baseUrl = useBuilderWatch("formValues.global.urlBase");
  const streamFieldPath: StreamPathFn = useCallback(
    <T extends string>(fieldPath: T) => `formValues.streams.${streamNum}.${fieldPath}` as const,
    [streamNum]
  );
  const selectedDecoder = useBuilderWatch(streamFieldPath("decoder"));

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
            showErrorIndicator={hasErrors([streamNum], "requester")}
          />
          <SchemaTab
            streamNum={streamNum}
            isSelected={streamTab === "schema"}
            onSelect={() => {
              setValue("streamTab", "schema");
              scrollToTop();
            }}
          />
        </FlexContainer>
        <RequestTypeSelector streamNum={streamNum} streamFieldPath={streamFieldPath} selectedValue="sync" />
      </FlexContainer>
      {streamTab === "requester" ? (
        <fieldset disabled={permission === "readOnly"} className={styles.fieldset}>
          <BuilderCard>
            <BuilderField
              type="jinja"
              path={streamFieldPath("urlPath")}
              manifestPath="HttpRequester.properties.path"
              preview={baseUrl ? (value) => `${baseUrl}${value}` : undefined}
              labelAction={<AssistButton assistKey="metadata" streamNum={streamNum} />}
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
          <RecordSelectorSection streamFieldPath={streamFieldPath} currentStreamIndex={streamNum} />
          <RequestOptionSection
            inline={false}
            basePath={streamFieldPath("requestOptions")}
            currentStreamIndex={streamNum}
          />
          <PaginationSection streamFieldPath={streamFieldPath} currentStreamIndex={streamNum} />
          <IncrementalSection streamFieldPath={streamFieldPath} currentStreamIndex={streamNum} />
          <ParentStreamsSection streamFieldPath={streamFieldPath} currentStreamIndex={streamNum} />
          <ParameterizedRequestsSection streamFieldPath={streamFieldPath} currentStreamIndex={streamNum} />
          <ErrorHandlerSection
            inline={false}
            basePath={streamFieldPath("errorHandler")}
            currentStreamIndex={streamNum}
          />
          <TransformationSection streamFieldPath={streamFieldPath} currentStreamIndex={streamNum} />
          <UnknownFieldsSection streamFieldPath={streamFieldPath} />
        </fieldset>
      ) : streamTab === "schema" ? (
        <BuilderCard className={styles.schemaEditor}>
          <SchemaEditor streamFieldPath={streamFieldPath} />
        </BuilderCard>
      ) : null}
    </>
  );
};

interface AsynchronousStreamProps {
  streamNum: number;
  scrollToTop: () => void;
}
const AsynchronousStream: React.FC<AsynchronousStreamProps> = ({ streamNum, scrollToTop }) => {
  const { formatMessage } = useIntl();
  const streamTab = useBuilderWatch("streamTab");
  const { setValue } = useFormContext();
  const { hasErrors } = useBuilderErrors();

  useEffect(() => {
    if (streamTab !== "requester" && streamTab !== "polling" && streamTab !== "download" && streamTab !== "schema") {
      setValue("streamTab", "requester");
    }
  }, [setValue, streamTab]);

  const streamFieldPath: StreamPathFn = useCallback(
    <T extends string>(fieldPath: T) => `formValues.streams.${streamNum}.${fieldPath}` as const,
    [streamNum]
  );
  const creationRequesterPath: CreationRequesterPathFn = useCallback(
    <T extends string>(fieldPath: T) => `formValues.streams.${streamNum}.creationRequester.${fieldPath}` as const,
    [streamNum]
  );
  const pollingRequesterPath: PollingRequesterPathFn = useCallback(
    <T extends string>(fieldPath: T) => `formValues.streams.${streamNum}.pollingRequester.${fieldPath}` as const,
    [streamNum]
  );
  const downloadRequesterPath: DownloadRequesterPathFn = useCallback(
    <T extends string>(fieldPath: T) => `formValues.streams.${streamNum}.downloadRequester.${fieldPath}` as const,
    [streamNum]
  );

  const selectedCreationDecoder = useBuilderWatch(creationRequesterPath("decoder"));
  const selectedDownloadDecoder = useBuilderWatch(downloadRequesterPath("decoder"));

  const tabContentKey = useMemo(() => `${streamNum}-${streamTab}`, [streamNum, streamTab]);

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
            showErrorIndicator={hasErrors([streamNum], "requester")}
          />
          <StreamTab
            data-testid="tag-tab-async-stream-polling"
            label={formatMessage({ id: "connectorBuilder.asyncStream.polling" })}
            isSelected={streamTab === "polling"}
            onSelect={() => {
              setValue("streamTab", "polling");
              scrollToTop();
            }}
            showErrorIndicator={hasErrors([streamNum], "polling")}
          />
          <StreamTab
            data-testid="tag-tab-async-stream-download"
            label={formatMessage({ id: "connectorBuilder.asyncStream.download" })}
            isSelected={streamTab === "download"}
            onSelect={() => {
              setValue("streamTab", "download");
              scrollToTop();
            }}
            showErrorIndicator={hasErrors([streamNum], "download")}
          />
          <SchemaTab
            streamNum={streamNum}
            isSelected={streamTab === "schema"}
            onSelect={() => {
              setValue("streamTab", "schema");
              scrollToTop();
            }}
          />
        </FlexContainer>
        <RequestTypeSelector streamNum={streamNum} streamFieldPath={streamFieldPath} selectedValue="async" />
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
          <RequestOptionSection
            inline={false}
            basePath={creationRequesterPath("requestOptions")}
            currentStreamIndex={streamNum}
          />
          <IncrementalSection streamFieldPath={creationRequesterPath} currentStreamIndex={streamNum} />
          <ParentStreamsSection streamFieldPath={creationRequesterPath} currentStreamIndex={streamNum} />
          <ParameterizedRequestsSection streamFieldPath={creationRequesterPath} currentStreamIndex={streamNum} />
          <ErrorHandlerSection
            inline={false}
            basePath={creationRequesterPath("errorHandler")}
            currentStreamIndex={streamNum}
          />
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
          <RequestOptionSection
            inline={false}
            basePath={pollingRequesterPath("requestOptions")}
            currentStreamIndex={streamNum}
          />
          <ErrorHandlerSection
            inline={false}
            basePath={pollingRequesterPath("errorHandler")}
            currentStreamIndex={streamNum}
          />
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
          <RecordSelectorSection streamFieldPath={downloadRequesterPath} currentStreamIndex={streamNum} />
          <RequestOptionSection
            inline={false}
            basePath={downloadRequesterPath("requestOptions")}
            currentStreamIndex={streamNum}
          />
          <PaginationSection streamFieldPath={downloadRequesterPath} currentStreamIndex={streamNum} />
          <TransformationSection streamFieldPath={downloadRequesterPath} currentStreamIndex={streamNum} />
          <ErrorHandlerSection
            inline={false}
            basePath={downloadRequesterPath("errorHandler")}
            currentStreamIndex={streamNum}
          />
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
  streamNum,
  streamFieldPath,
  selectedValue,
}: {
  streamNum: number;
  streamFieldPath: StreamPathFn;
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
            setValue(`formValues.streams.${streamNum}`, {
              ...DEFAULT_BUILDER_STREAM_VALUES,
              id,
              name,
            });
          } else if (newValue === "async") {
            setValue(`formValues.streams.${streamNum}`, {
              ...DEFAULT_BUILDER_ASYNC_STREAM_VALUES,
              id,
              name,
            });
          }
          closeConfirmationModal();
        },
      });
    },
    [selectedValue, openConfirmationModal, closeConfirmationModal, setValue, streamNum, id, name]
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
  streamNum,
  isSelected,
  onSelect,
}: {
  streamNum: number;
  isSelected: boolean;
  onSelect: () => void;
}) => {
  const { formatMessage } = useIntl();
  const {
    schemaWarnings: { incompatibleSchemaErrors, schemaDifferences },
  } = useConnectorBuilderTestRead();
  const { hasErrors } = useBuilderErrors();
  const autoImportSchema = useAutoImportSchema(streamNum);

  return (
    <StreamTab
      data-testid="tag-tab-stream-schema"
      label={formatMessage({ id: "connectorBuilder.streamSchema" })}
      isSelected={isSelected}
      onSelect={() => onSelect()}
      showErrorIndicator={hasErrors([streamNum], "schema")}
      showSchemaConflictIndicator={schemaDifferences && !autoImportSchema}
      schemaErrors={incompatibleSchemaErrors}
    />
  );
};

const SchemaEditor = ({ streamFieldPath }: { streamFieldPath: StreamPathFn }) => {
  const { formatMessage } = useIntl();
  const analyticsService = useAnalyticsService();
  const { permission, streamNames } = useConnectorBuilderFormState();
  const autoImportSchemaFieldPath = streamFieldPath("autoImportSchema");
  const autoImportSchema = useBuilderWatch(autoImportSchemaFieldPath);
  const schemaFieldPath = streamFieldPath("schema");
  const schema = useBuilderWatch(schemaFieldPath);
  const testStreamIndex = useBuilderWatch("testStreamIndex");
  const { setValue } = useFormContext();
  const path = streamFieldPath("schema");
  const { errors } = useFormState({ name: path });
  const error = get(errors, path);
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
        disabled={(error && !streamRead.data?.inferred_schema) || permission === "readOnly"}
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
            setValue(path, formattedJson);
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.OVERWRITE_SCHEMA, {
              actionDescription: "Declared schema overwritten by detected schema",
              stream_name: streamNames[testStreamIndex],
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
            readOnly={permission === "readOnly"}
            key={schemaFieldPath}
            value={schema || ""}
            language="json"
            onChange={(val: string | undefined) => {
              setValue(path, val, {
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

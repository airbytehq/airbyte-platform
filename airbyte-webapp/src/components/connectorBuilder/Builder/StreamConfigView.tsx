import classNames from "classnames";
import isEqual from "lodash/isEqual";
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControl } from "components/forms";
import { SchemaFormControl } from "components/forms/SchemaForm/Controls/SchemaFormControl";
import { SchemaFormRemainingFields } from "components/forms/SchemaForm/SchemaFormRemainingFields";
import Indicator from "components/Indicator";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { Collapsible } from "components/ui/Collapsible";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { Message } from "components/ui/Message";
import { Pre } from "components/ui/Pre";
import { Text } from "components/ui/Text";

import {
  AsyncRetrieverType,
  CustomRetrieverType,
  DeclarativeComponentSchemaStreamsItem,
  DeclarativeStream,
  DeclarativeStreamSchemaLoader,
  DeclarativeStreamType,
  DynamicDeclarativeStream,
  InlineSchemaLoaderType,
  SimpleRetrieverType,
  StateDelegatingStreamType,
} from "core/api/types/ConnectorManifest";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import {
  BuilderView,
  useConnectorBuilderPermission,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderConfigView } from "./BuilderConfigView";
import { ParentStreamSelector } from "./overrides";
import styles from "./StreamConfigView.module.scss";
import {
  DEFAULT_SYNC_STREAM,
  DEFAULT_ASYNC_STREAM,
  DEFAULT_CUSTOM_STREAM,
  DEFAULT_SCHEMA_LOADER_SCHEMA,
} from "../constants";
import { SchemaConflictIndicator } from "../SchemaConflictIndicator";
import { StreamId, BuilderStreamTab } from "../types";
import { useAutoImportSchema } from "../useAutoImportSchema";
import { useBuilderErrors } from "../useBuilderErrors";
import { useBuilderWatch } from "../useBuilderWatch";
import { useUpdateMetadata } from "../useUpdateMetadata";
import { formatJson, getStreamFieldPath } from "../utils";

interface StreamConfigViewProps {
  streamId: StreamId;
  scrollToTop: () => void;
}

export const StreamConfigView: React.FC<StreamConfigViewProps> = React.memo(({ streamId, scrollToTop }) => {
  const { formatMessage } = useIntl();
  const analyticsService = useAnalyticsService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { setValue, getValues } = useFormContext();

  const streamFieldPath = useCallback(
    (fieldPath?: string) => getStreamFieldPath(streamId, fieldPath, true),
    [streamId]
  );
  const streamName = useBuilderWatch(streamFieldPath("name")) as string | undefined;
  const streamType = useBuilderWatch(streamFieldPath("type")) as
    | DeclarativeStreamType
    | StateDelegatingStreamType
    | undefined;
  const streamRetrieverType = useBuilderWatch(streamFieldPath("retriever.type")) as
    | SimpleRetrieverType
    | AsyncRetrieverType
    | CustomRetrieverType
    | undefined;

  const retrievalType: RetrievalType | null = useMemo(() => {
    if (streamType === StateDelegatingStreamType.StateDelegatingStream) {
      return null;
    }
    return streamRetrieverType === SimpleRetrieverType.SimpleRetriever
      ? "sync"
      : streamRetrieverType === AsyncRetrieverType.AsyncRetriever
      ? "async"
      : "custom";
  }, [streamType, streamRetrieverType]);

  const handleDelete = useCallback(() => {
    openConfirmationModal({
      text: "connectorBuilder.deleteStreamModal.text",
      title: "connectorBuilder.deleteStreamModal.title",
      submitButtonText: "connectorBuilder.deleteStreamModal.submitButton",
      onSubmit: () => {
        if (streamId.type !== "stream") {
          return;
        }
        const streams: DeclarativeComponentSchemaStreamsItem[] = getValues("manifest.streams");
        const updatedStreams = updateStreamsAndRefsAfterDelete(streams, streamId.index);
        const streamToSelect = streamId.index >= updatedStreams.length ? updatedStreams.length - 1 : streamId.index;
        const viewToSelect: BuilderView =
          updatedStreams.length === 0 ? { type: "global" } : { type: "stream", index: streamToSelect };
        setValue("manifest.streams", updatedStreams);
        setValue("view", viewToSelect);
        closeConfirmationModal();
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_DELETE, {
          actionDescription: "Stream deleted",
          stream_id: streamId.index,
          stream_name: streamName,
        });
      },
    });
  }, [analyticsService, closeConfirmationModal, streamName, getValues, openConfirmationModal, setValue, streamId]);

  useUpdateMetadata(streamId);

  const generatedStreamMessage = useMemo(
    () =>
      streamId.type === "generated_stream" ? (
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
                      const dynamicStreams: DynamicDeclarativeStream[] = getValues("manifest.dynamic_streams");
                      setValue("view", {
                        type: "dynamic_stream",
                        index: dynamicStreams.findIndex((stream) => stream.name === streamId.dynamicStreamName),
                      });
                    }}
                  >
                    <Text bold>{streamId.dynamicStreamName}</Text>
                  </Button>
                ),
              }}
            />
          }
        />
      ) : null,
    [getValues, setValue, streamId]
  );

  return (
    <BuilderConfigView className={styles.relative}>
      {streamId.type === "stream" && (
        <FlexContainer justifyContent="flex-end" className={classNames(styles.titleBar)} alignItems="center">
          <SchemaFormControl
            path={getStreamFieldPath(streamId, "name")}
            titleOverride={null}
            className={styles.streamNameInput}
            placeholder={formatMessage({ id: "connectorBuilder.streamName.placeholder" })}
          />

          <Button type="button" variant="danger" onClick={handleDelete}>
            <FormattedMessage id="connectorBuilder.deleteStreamModal.title" />
          </Button>
        </FlexContainer>
      )}
      {retrievalType === "sync" ? (
        <SynchronousStream streamId={streamId} scrollToTop={scrollToTop} message={generatedStreamMessage} />
      ) : retrievalType === "async" ? (
        <AsynchronousStream streamId={streamId} scrollToTop={scrollToTop} message={generatedStreamMessage} />
      ) : null}
    </BuilderConfigView>
  );
});

StreamConfigView.displayName = "StreamConfigView";

interface SynchronousStreamProps {
  streamId: StreamId;
  scrollToTop: () => void;
  message: React.ReactNode | null;
}
const SynchronousStream: React.FC<SynchronousStreamProps> = ({ streamId, scrollToTop, message }) => {
  const { formatMessage } = useIntl();
  const permission = useConnectorBuilderPermission();
  const streamTab = useBuilderWatch("streamTab");
  const view = useBuilderWatch("view");
  const { setValue } = useFormContext();

  const streamFieldPath = useCallback(
    (fieldPath?: string) => getStreamFieldPath(streamId, fieldPath, true),
    [streamId]
  );
  const name = useBuilderWatch(streamFieldPath("name")) as string | undefined;

  useEffect(() => {
    if (isEqual(view, streamId) && streamTab !== "requester" && streamTab !== "schema") {
      setValue("streamTab", "requester");
    }
  }, [setValue, streamId, streamTab, view]);

  return (
    <>
      <FlexContainer className={styles.sticky} direction="column">
        {message}
        <FlexContainer justifyContent="space-between" alignItems="center">
          <FlexContainer>
            <StreamTab
              data-testid="tag-tab-stream-configuration"
              streamId={streamId}
              builderStreamTab="requester"
              label={formatMessage({ id: "connectorBuilder.streamConfiguration" })}
              isSelected={streamTab === "requester"}
              onSelect={() => {
                setValue("streamTab", "requester");
                scrollToTop();
              }}
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
          <RetrievalTypeSelector
            streamFieldPath={streamFieldPath}
            streamName={name ?? ""}
            selectedValue="sync"
            disabled={streamId.type === "generated_stream"}
          />
        </FlexContainer>
      </FlexContainer>
      <fieldset disabled={permission === "readOnly"} className={styles.fieldset}>
        <FlexContainer
          direction="column"
          className={classNames({ [styles.hidden]: streamTab !== "requester" })}
          data-stream-tab="requester"
        >
          {streamId.type === "dynamic_stream" && (
            <StreamCard>
              <SchemaFormControl path={streamFieldPath("name")} isRequired />
            </StreamCard>
          )}
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.requester.url")} isRequired />
            <SchemaFormControl path={streamFieldPath("retriever.requester.url_base")} />
            <SchemaFormControl path={streamFieldPath("retriever.requester.path")} />
            <SchemaFormControl path={streamFieldPath("retriever.requester.http_method")} />
            <SchemaFormControl path={streamFieldPath("retriever.decoder")} />
            <SchemaFormControl path={streamFieldPath("retriever.record_selector")} nonAdvancedFields={["extractor"]} />
            <SchemaFormControl path={streamFieldPath("primary_key")} />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl
              path={streamFieldPath("retriever.requester.authenticator")}
              nonAdvancedFields={NON_ADVANCED_AUTH_FIELDS}
            />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.requester.request_parameters")} />
            <SchemaFormControl path={streamFieldPath("retriever.requester.request_headers")} />
            <SchemaFormControl path={streamFieldPath("retriever.requester.request_body")} />
            <SchemaFormControl path={streamFieldPath("retriever.requester.request_body_json")} />
            <SchemaFormControl path={streamFieldPath("retriever.requester.request_body_data")} />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.paginator")} />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl
              path={streamFieldPath("incremental_sync")}
              nonAdvancedFields={NON_ADVANCED_INCREMENTAL_FIELDS}
            />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl
              path={streamFieldPath("retriever.partition_router")}
              overrideByPath={{
                [streamFieldPath("retriever.partition_router.*.parent_stream_configs.*.stream")]: (path) => (
                  <ParentStreamSelector path={path} currentStreamName={name} />
                ),
                [streamFieldPath("retriever.partition_router.parent_stream_configs.*.stream")]: (path) => (
                  <ParentStreamSelector path={path} currentStreamName={name} />
                ),
              }}
            />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.requester.error_handler")} />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("transformations")} />
          </StreamCard>
          <Card>
            <CollapsedControls streamId={streamId}>
              <SchemaFormRemainingFields path={streamFieldPath("retriever.requester")} />
              <SchemaFormRemainingFields path={streamFieldPath("retriever")} />
              <SchemaFormRemainingFields path={streamFieldPath()} />
            </CollapsedControls>
          </Card>
        </FlexContainer>
        <FlexContainer
          direction="column"
          className={classNames({ [styles.hidden]: streamTab !== "schema" })}
          data-stream-tab="schema"
        >
          <SchemaEditor streamId={streamId} streamFieldPath={streamFieldPath} />
        </FlexContainer>
      </fieldset>
    </>
  );
};

interface AsynchronousStreamProps {
  streamId: StreamId;
  scrollToTop: () => void;
  message: React.ReactNode | null;
}
const AsynchronousStream: React.FC<AsynchronousStreamProps> = ({ streamId, scrollToTop, message }) => {
  const { formatMessage } = useIntl();
  const permission = useConnectorBuilderPermission();
  const streamTab = useBuilderWatch("streamTab");
  const view = useBuilderWatch("view");
  const { setValue } = useFormContext();

  const streamFieldPath = useCallback(
    (fieldPath?: string) => getStreamFieldPath(streamId, fieldPath, true),
    [streamId]
  );
  const name = useBuilderWatch(streamFieldPath("name")) as string | undefined;

  useEffect(() => {
    if (
      isEqual(view, streamId) &&
      streamTab !== "requester" &&
      streamTab !== "polling" &&
      streamTab !== "download" &&
      streamTab !== "schema"
    ) {
      setValue("streamTab", "requester");
    }
  }, [setValue, streamId, streamTab, view]);

  return (
    <>
      <FlexContainer className={styles.sticky} direction="column">
        {message}
        <FlexContainer justifyContent="space-between" alignItems="center">
          <FlexContainer>
            <StreamTab
              data-testid="tag-tab-async-stream-creation"
              streamId={streamId}
              builderStreamTab="requester"
              label={formatMessage({ id: "connectorBuilder.asyncStream.creation" })}
              isSelected={streamTab === "requester"}
              onSelect={() => {
                setValue("streamTab", "requester");
                scrollToTop();
              }}
            />
            <StreamTab
              data-testid="tag-tab-async-stream-polling"
              streamId={streamId}
              builderStreamTab="polling"
              label={formatMessage({ id: "connectorBuilder.asyncStream.polling" })}
              isSelected={streamTab === "polling"}
              onSelect={() => {
                setValue("streamTab", "polling");
                scrollToTop();
              }}
            />
            <StreamTab
              data-testid="tag-tab-async-stream-download"
              streamId={streamId}
              builderStreamTab="download"
              label={formatMessage({ id: "connectorBuilder.asyncStream.download" })}
              isSelected={streamTab === "download"}
              onSelect={() => {
                setValue("streamTab", "download");
                scrollToTop();
              }}
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
          <RetrievalTypeSelector
            streamFieldPath={streamFieldPath}
            streamName={name ?? ""}
            selectedValue="async"
            disabled={streamId.type === "generated_stream"}
          />
        </FlexContainer>
      </FlexContainer>
      <fieldset disabled={permission === "readOnly"} className={styles.fieldset}>
        <FlexContainer
          direction="column"
          className={classNames({ [styles.hidden]: streamTab !== "requester" })}
          data-stream-tab="requester"
        >
          {streamId.type === "dynamic_stream" && (
            <StreamCard>
              <SchemaFormControl path={streamFieldPath("name")} isRequired />
            </StreamCard>
          )}
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.creation_requester.url")} isRequired />
            <SchemaFormControl path={streamFieldPath("retriever.creation_requester.url_base")} />
            <SchemaFormControl path={streamFieldPath("retriever.creation_requester.path")} />
            <SchemaFormControl path={streamFieldPath("retriever.creation_requester.http_method")} />
            <SchemaFormControl path={streamFieldPath("retriever.decoder")} />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl
              path={streamFieldPath("retriever.creation_requester.authenticator")}
              nonAdvancedFields={NON_ADVANCED_AUTH_FIELDS}
            />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.creation_requester.request_parameters")} />
            <SchemaFormControl path={streamFieldPath("retriever.creation_requester.request_headers")} />
            <SchemaFormControl path={streamFieldPath("retriever.creation_requester.request_body")} />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl
              path={streamFieldPath("incremental_sync")}
              nonAdvancedFields={NON_ADVANCED_INCREMENTAL_FIELDS}
            />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl
              path={streamFieldPath("retriever.partition_router")}
              overrideByPath={{
                [streamFieldPath("retriever.partition_router.*.parent_stream_configs.*.stream")]: (path) => (
                  <ParentStreamSelector path={path} currentStreamName={name} />
                ),
                [streamFieldPath("retriever.partition_router.parent_stream_configs.*.stream")]: (path) => (
                  <ParentStreamSelector path={path} currentStreamName={name} />
                ),
              }}
            />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.creation_requester.error_handler")} />
          </StreamCard>
          <Card>
            <CollapsedControls streamId={streamId}>
              <SchemaFormRemainingFields path={streamFieldPath("retriever.creation_requester")} />
              <SchemaFormRemainingFields path={streamFieldPath()} />
            </CollapsedControls>
          </Card>
        </FlexContainer>
        <FlexContainer
          direction="column"
          className={classNames({ [styles.hidden]: streamTab !== "polling" })}
          data-stream-tab="polling"
        >
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.polling_requester.url")} isRequired />
            <SchemaFormControl path={streamFieldPath("retriever.polling_requester.url_base")} />
            <SchemaFormControl path={streamFieldPath("retriever.polling_requester.path")} />
            <SchemaFormControl path={streamFieldPath("retriever.polling_requester.http_method")} />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl
              path={streamFieldPath("retriever.polling_requester.authenticator")}
              nonAdvancedFields={NON_ADVANCED_AUTH_FIELDS}
            />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.status_extractor")} />
            <SchemaFormControl path={streamFieldPath("retriever.status_mapping")} />
            <SchemaFormControl path={streamFieldPath("retriever.download_target_extractor")} />
            <SchemaFormControl path={streamFieldPath("retriever.polling_job_timeout")} />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.polling_requester.request_parameters")} />
            <SchemaFormControl path={streamFieldPath("retriever.polling_requester.request_headers")} />
            <SchemaFormControl path={streamFieldPath("retriever.polling_requester.request_body")} />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.polling_requester.error_handler")} />
          </StreamCard>
          <Card>
            <CollapsedControls streamId={streamId}>
              <SchemaFormRemainingFields path={streamFieldPath("retriever.polling_requester")} />
            </CollapsedControls>
          </Card>
        </FlexContainer>
        <FlexContainer
          direction="column"
          className={classNames({ [styles.hidden]: streamTab !== "download" })}
          data-stream-tab="download"
        >
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.download_requester.url")} isRequired />
            <SchemaFormControl path={streamFieldPath("retriever.download_requester.url_base")} />
            <SchemaFormControl path={streamFieldPath("retriever.download_requester.path")} />
            <SchemaFormControl path={streamFieldPath("retriever.download_requester.http_method")} />
            <SchemaFormControl path={streamFieldPath("retriever.download_decoder")} />
            <SchemaFormControl path={streamFieldPath("retriever.record_selector")} nonAdvancedFields={["extractor"]} />
            <SchemaFormControl path={streamFieldPath("primary_key")} />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl
              path={streamFieldPath("retriever.download_requester.authenticator")}
              nonAdvancedFields={NON_ADVANCED_AUTH_FIELDS}
            />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.download_requester.request_parameters")} />
            <SchemaFormControl path={streamFieldPath("retriever.download_requester.request_headers")} />
            <SchemaFormControl path={streamFieldPath("retriever.download_requester.request_body")} />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.download_paginator")} />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("transformations")} />
          </StreamCard>
          <StreamCard>
            <SchemaFormControl path={streamFieldPath("retriever.download_requester.error_handler")} />
          </StreamCard>
          <Card>
            <CollapsedControls streamId={streamId}>
              <SchemaFormRemainingFields path={streamFieldPath("retriever.download_requester")} />
              <SchemaFormRemainingFields path={streamFieldPath("retriever")} />
            </CollapsedControls>
          </Card>
        </FlexContainer>
        <FlexContainer
          direction="column"
          className={classNames({ [styles.hidden]: streamTab !== "schema" })}
          data-stream-tab="schema"
        >
          <SchemaEditor streamId={streamId} streamFieldPath={streamFieldPath} />
        </FlexContainer>
      </fieldset>
    </>
  );
};

type RetrievalType = "sync" | "async" | "custom";
const RetrievalTypeSelector = ({
  streamFieldPath,
  streamName,
  selectedValue,
  disabled,
}: {
  streamFieldPath: (fieldPath?: string) => string;
  streamName: string;
  selectedValue: RetrievalType;
  disabled?: boolean;
}) => {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { setValue, getValues } = useFormContext();

  const handleSelect = useCallback(
    (newValue: RetrievalType) => {
      if (newValue === selectedValue) {
        return;
      }

      openConfirmationModal({
        title:
          newValue === "sync"
            ? "connectorBuilder.retrievalType.confirm.title.sync"
            : newValue === "async"
            ? "connectorBuilder.retrievalType.confirm.title.async"
            : "connectorBuilder.retrievalType.confirm.title.custom",
        text: "connectorBuilder.retrievalType.confirm.text",
        submitButtonText:
          newValue === "sync"
            ? "connectorBuilder.retrievalType.confirm.submit.sync"
            : newValue === "async"
            ? "connectorBuilder.retrievalType.confirm.submit.async"
            : "connectorBuilder.retrievalType.confirm.submit.custom",
        submitButtonVariant: "primary",
        cancelButtonText: "connectorBuilder.retrievalType.confirm.cancel",
        onSubmit: () => {
          const newStreamValues: DeclarativeStream = {
            name: streamName,
            ...(newValue === "sync"
              ? DEFAULT_SYNC_STREAM
              : newValue === "async"
              ? DEFAULT_ASYNC_STREAM
              : DEFAULT_CUSTOM_STREAM),
          };
          const newStreamValuesWithUrl =
            selectedValue === "sync"
              ? setUrlOnStream(getValues(streamFieldPath("retriever.requester.url")), newStreamValues)
              : selectedValue === "async"
              ? setUrlOnStream(getValues(streamFieldPath("retriever.creation_requester.url")), newStreamValues)
              : newStreamValues;
          setValue(streamFieldPath(), newStreamValuesWithUrl);
          closeConfirmationModal();
        },
      });
    },
    [selectedValue, openConfirmationModal, streamName, getValues, streamFieldPath, setValue, closeConfirmationModal]
  );

  return (
    <FlexContainer alignItems="center">
      <Text color="grey500" align="right">
        <FormattedMessage id="connectorBuilder.retrievalType" />:
      </Text>
      <ListBox<RetrievalType>
        selectedValue={selectedValue}
        onSelect={handleSelect}
        options={[
          {
            label: <FormattedMessage id="connectorBuilder.retrievalType.sync" />,
            value: "sync",
          },
          {
            label: <FormattedMessage id="connectorBuilder.retrievalType.async" />,
            value: "async",
          },
          {
            label: <FormattedMessage id="connectorBuilder.retrievalType.custom" />,
            value: "custom",
          },
        ]}
        placement="bottom-end"
        adaptiveWidth={false}
        flip={false}
        buttonClassName={styles.requestTypeButton}
        isDisabled={disabled}
      />
    </FlexContainer>
  );
};

const setUrlOnStream = (url: string, declarativeStream: DeclarativeStream) => {
  if (declarativeStream.retriever.type === SimpleRetrieverType.SimpleRetriever) {
    declarativeStream.retriever.requester.url = url;
  } else if (declarativeStream.retriever.type === AsyncRetrieverType.AsyncRetriever) {
    declarativeStream.retriever.creation_requester.url = url;
    declarativeStream.retriever.polling_requester.url = url;
    declarativeStream.retriever.download_requester.url = url;
  }
  return declarativeStream;
};

const StreamTab = ({
  streamId,
  builderStreamTab,
  isSelected,
  label,
  onSelect,
  showSchemaConflictIndicator,
  schemaErrors,
  "data-testid": testId,
}: {
  streamId: StreamId;
  builderStreamTab: BuilderStreamTab;
  isSelected: boolean;
  label: string;
  onSelect: () => void;
  showSchemaConflictIndicator?: boolean;
  schemaErrors?: string[];
  "data-testid": string;
}) => {
  const { hasErrors } = useBuilderErrors();
  const showErrorIndicator = hasErrors([streamId], builderStreamTab);

  return (
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
};

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
  const autoImportSchema = useAutoImportSchema(streamId);

  return (
    <StreamTab
      data-testid="tag-tab-stream-schema"
      streamId={streamId}
      builderStreamTab="schema"
      label={formatMessage({ id: "connectorBuilder.streamSchema" })}
      isSelected={isSelected}
      onSelect={() => onSelect()}
      showSchemaConflictIndicator={streamId.type !== "generated_stream" && schemaDifferences && !autoImportSchema}
      schemaErrors={incompatibleSchemaErrors}
    />
  );
};

const SchemaEditor = ({
  streamId,
  streamFieldPath,
}: {
  streamId: StreamId;
  streamFieldPath: (fieldPath?: string) => string;
}) => {
  const { formatMessage } = useIntl();
  const { setValue } = useFormContext();
  const { streamRead } = useConnectorBuilderTestRead();
  // Get stream name from getStreamFieldPath instead of streamFieldPath, because streamFieldPath
  // returns stream template paths, but we want the name of the dynamic stream since that is what
  // autoImportSchema is tied to.
  const streamName = useBuilderWatch(getStreamFieldPath(streamId, "name")) as string | undefined;
  const schemaLoaderPath = streamFieldPath("schema_loader");
  const autoImportSchemaPath = `manifest.metadata.autoImportSchema.${streamName}`;
  const autoImportSchema = useBuilderWatch(autoImportSchemaPath);
  const inferredSchema = streamRead.data?.inferred_schema ?? DEFAULT_SCHEMA_LOADER_SCHEMA;
  const schemaLoader = useBuilderWatch(schemaLoaderPath) as DeclarativeStreamSchemaLoader | undefined;

  if (!streamName) {
    // Use SchemaFormControl with override so that the schema_loader is not rendered elsewhere
    return (
      <SchemaFormControl
        path={schemaLoaderPath}
        overrideByPath={{
          [schemaLoaderPath]: () => (
            <Message type="warning" text={formatMessage({ id: "connectorBuilder.streamSchema.noStreamName" })} />
          ),
        }}
      />
    );
  }

  return (
    <Card className={classNames({ [styles.card]: !autoImportSchema })}>
      {streamId.type !== "generated_stream" && (
        <FormControl
          label={formatMessage({ id: "connectorBuilder.autoImportSchema.label" })}
          name={autoImportSchemaPath}
          fieldType="switch"
          labelTooltip={
            <FormattedMessage id="connectorBuilder.autoImportSchema.tooltip" values={{ br: () => <br /> }} />
          }
          onChange={(e) => {
            if (e.target.checked) {
              setValue(schemaLoaderPath, {
                type: InlineSchemaLoaderType.InlineSchemaLoader,
                schema: inferredSchema,
              });
            }
          }}
        />
      )}
      <SchemaFormControl
        path={schemaLoaderPath}
        overrideByPath={
          streamId.type !== "generated_stream" &&
          autoImportSchema &&
          !Array.isArray(schemaLoader) &&
          schemaLoader?.type === InlineSchemaLoaderType.InlineSchemaLoader
            ? {
                [schemaLoaderPath]: () => (
                  <div className={styles.autoSchemaContainer}>
                    <Pre>{formatJson(schemaLoader.schema, true)}</Pre>
                  </div>
                ),
              }
            : undefined
        }
      />
    </Card>
  );
};

const StreamCard: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  return <Card className={styles.card}>{children}</Card>;
};

const NON_ADVANCED_AUTH_FIELDS = [
  "api_token",
  "header",
  "username",
  "password",
  "inject_into",
  "client_id",
  "client_secret",
  "refresh_token",
  "access_token_value",
  "scopes",
  "grant_type",
  "secret_key",
  "algorithm",
  "jwt_headers",
  "jwt_payload",
  "login_requester.url",
  "login_requester.http_method",
  "login_requester.authenticator",
  "login_requester.request_parameters",
  "login_requester.request_headers",
  "login_requester.request_body",
  "session_token_path",
  "expiration_duration",
  "request_authentication",
  "authenticator_selection_path",
  "authenticators",
  "class_name",
];

const NON_ADVANCED_INCREMENTAL_FIELDS = [
  "cursor_field",
  "cursor_datetime_formats",
  "datetime_format",
  "start_datetime.datetime",
  "start_datetime.datetime_format",
  "start_time_option",
  "end_datetime.datetime",
  "end_datetime.datetime_format",
  "end_time_option",
  "datetime_format",
  "cursor_granularity",
  "step",
];

interface CollapsedControlsProps {
  streamId: StreamId;
}
const CollapsedControls: React.FC<React.PropsWithChildren<CollapsedControlsProps>> = ({ streamId, children }) => {
  const { getErrorPaths } = useBuilderErrors();

  const containerRef = useRef<HTMLDivElement>(null);
  const errorPaths = getErrorPaths(streamId);
  const [hasChildError, setHasChildError] = useState(false);

  useEffect(() => {
    if (containerRef.current && errorPaths.length > 0) {
      const selector = errorPaths.map((path) => `[data-field-path="${path}"]`).join(", "); // Combine with commas for "OR" logic

      const target = containerRef.current.querySelector(selector);
      setHasChildError(!!target);
    } else if (errorPaths.length === 0) {
      setHasChildError(false);
    }
  }, [errorPaths]);

  return (
    <div ref={containerRef}>
      <Collapsible label="Advanced" initiallyOpen={hasChildError} showErrorIndicator={hasChildError}>
        {children}
      </Collapsible>
    </div>
  );
};

/**
 * Removes the stream at the given index from the streams array, and updates $refs accordingly:
 * - If the $ref points to the deleted stream, it is replaced with undefined
 * - If the $ref points to a stream after the deleted stream, the $ref is updated to point to one stream index lower,
 *   as that will now hold the stream that was previously pointed to.
 */
const updateStreamsAndRefsAfterDelete = (
  streams: DeclarativeComponentSchemaStreamsItem[],
  deletedStreamIndex: number
) => {
  function updateRefs<T>(obj: T): T | undefined {
    if (!obj || typeof obj !== "object") {
      return obj;
    }

    if (Array.isArray(obj)) {
      return obj.map(updateRefs) as T;
    }

    if ("$ref" in obj && typeof obj.$ref === "string") {
      // check if $ref points to a stream index and capture the index and the suffix
      const match = obj.$ref.match(/#\/streams\/(\d+)(?:\/(.*)|$)/);
      if (match) {
        const streamIndex = Number(match[1]);
        if (streamIndex === deletedStreamIndex) {
          return undefined;
        }
        if (streamIndex > deletedStreamIndex) {
          const newRef = match[2] ? `#/streams/${streamIndex - 1}/${match[2]}` : `#/streams/${streamIndex - 1}`;
          return {
            ...obj,
            $ref: newRef,
          };
        }
      }
      return obj;
    }

    const result = {} as Record<string, unknown>;
    for (const [key, value] of Object.entries(obj)) {
      result[key] = updateRefs(value);
    }
    return result as T;
  }

  return streams.filter((_, index) => index !== deletedStreamIndex).map(updateRefs);
};

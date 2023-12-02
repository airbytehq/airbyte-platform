import classNames from "classnames";
import React, { useCallback, useMemo, useState } from "react";
import { get, useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import Indicator from "components/Indicator";
import { Button } from "components/ui/Button";
import { CodeEditor } from "components/ui/CodeEditor";
import { Icon } from "components/ui/Icon";
import { Pre } from "components/ui/Pre";
import { Text } from "components/ui/Text";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { BuilderView, useConnectorBuilderTestRead } from "services/connectorBuilder/ConnectorBuilderStateService";

import { AddStreamButton } from "./AddStreamButton";
import { BuilderCard } from "./BuilderCard";
import { BuilderConfigView } from "./BuilderConfigView";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderTitle } from "./BuilderTitle";
import { ErrorHandlerSection } from "./ErrorHandlerSection";
import { IncrementalSection } from "./IncrementalSection";
import { getOptionsByManifest } from "./manifestHelpers";
import { PaginationSection } from "./PaginationSection";
import { ParameterizedRequestsSection } from "./ParameterizedRequestsSection";
import { ParentStreamsSection } from "./ParentStreamsSection";
import { RequestOptionSection } from "./RequestOptionSection";
import styles from "./StreamConfigView.module.scss";
import { TransformationSection } from "./TransformationSection";
import { SchemaConflictIndicator } from "../SchemaConflictIndicator";
import { BuilderStream, StreamPathFn, isEmptyOrDefault, useBuilderWatch } from "../types";
import { useAutoImportSchema } from "../useAutoImportSchema";
import { formatJson } from "../utils";

interface StreamConfigViewProps {
  streamNum: number;
  hasMultipleStreams: boolean;
}

export const StreamConfigView: React.FC<StreamConfigViewProps> = React.memo(({ streamNum, hasMultipleStreams }) => {
  const { formatMessage } = useIntl();

  const [selectedTab, setSelectedTab] = useState<"configuration" | "schema">("configuration");
  const streamPath = `formValues.streams.${streamNum}` as const;
  const streamFieldPath: StreamPathFn = useCallback(
    <T extends string>(fieldPath: T) => `${streamPath}.${fieldPath}` as const,
    [streamPath]
  );

  return (
    <BuilderConfigView
      heading={formatMessage({ id: "connectorBuilder.stream" })}
      className={hasMultipleStreams ? styles.multiStreams : undefined}
    >
      {/* Not using intl for the labels and tooltips in this component in order to keep maintainence simple */}
      <BuilderTitle
        path={streamFieldPath("name")}
        label={formatMessage({ id: "connectorBuilder.streamConfigView.name" })}
        size="md"
      />
      <StreamControls
        streamNum={streamNum}
        selectedTab={selectedTab}
        setSelectedTab={setSelectedTab}
        streamFieldPath={streamFieldPath}
      />
      {selectedTab === "configuration" ? (
        <>
          <BuilderCard>
            <BuilderFieldWithInputs
              type="string"
              path={streamFieldPath("urlPath")}
              manifestPath="HttpRequester.properties.path"
            />
            <BuilderField
              type="enum"
              path={streamFieldPath("httpMethod")}
              options={getOptionsByManifest("HttpRequester.properties.http_method.anyOf.1")}
              manifestPath="HttpRequester.properties.http_method"
            />
            <BuilderField
              type="array"
              path={streamFieldPath("fieldPointer")}
              label={formatMessage({ id: "connectorBuilder.streamConfigView.fieldPointer" })}
              manifestPath="DpathExtractor.properties.field_path"
              optional
            />
            <BuilderField
              type="array"
              path={streamFieldPath("primaryKey")}
              label={formatMessage({ id: "connectorBuilder.streamConfigView.primaryKey.label" })}
              tooltip={formatMessage({ id: "connectorBuilder.streamConfigView.primaryKey.tooltip" })}
              directionalStyle={false}
              optional
            />
          </BuilderCard>
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
        </>
      ) : (
        <BuilderCard className={styles.schemaEditor}>
          <SchemaEditor streamFieldPath={streamFieldPath} />
        </BuilderCard>
      )}
    </BuilderConfigView>
  );
});

StreamConfigView.displayName = "StreamConfigView";

const StreamControls = ({
  streamNum,
  selectedTab,
  setSelectedTab,
  streamFieldPath,
}: {
  streamNum: number;
  streamFieldPath: StreamPathFn;
  setSelectedTab: (tab: "configuration" | "schema") => void;
  selectedTab: "configuration" | "schema";
}) => {
  const analyticsService = useAnalyticsService();
  const { formatMessage } = useIntl();
  const streams = useBuilderWatch("formValues.streams");
  const { setValue } = useFormContext();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const {
    schemaWarnings: { incompatibleSchemaErrors, schemaDifferences },
  } = useConnectorBuilderTestRead();
  const { errors } = useFormState({ name: streamFieldPath("schema") });
  const error = get(errors, streamFieldPath("schema"));
  const hasSchemaErrors = Boolean(error);
  const autoImportSchema = useAutoImportSchema(streamNum);

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
    <div className={styles.controls}>
      <StreamTab
        data-testid="tag-tab-stream-configuration"
        label={formatMessage({ id: "connectorBuilder.streamConfiguration" })}
        selected={selectedTab === "configuration"}
        onSelect={() => setSelectedTab("configuration")}
      />
      <StreamTab
        data-testid="tag-tab-stream-schema"
        label={formatMessage({ id: "connectorBuilder.streamSchema" })}
        selected={selectedTab === "schema"}
        onSelect={() => setSelectedTab("schema")}
        showErrorIndicator={hasSchemaErrors}
        showSchemaConflictIndicator={schemaDifferences && !autoImportSchema}
        schemaErrors={incompatibleSchemaErrors}
      />
      <AddStreamButton
        onAddStream={(addedStreamNum) => {
          setValue("view", addedStreamNum);
        }}
        initialValues={streams[streamNum]}
        button={
          <button className={styles.controlButton} type="button">
            <Icon type="copy" />
          </button>
        }
        modalTitle={formatMessage({ id: "connectorBuilder.copyStreamModal.title" }, { name: streams[streamNum].name })}
      />
      <button className={classNames(styles.deleteButton, styles.controlButton)} type="button" onClick={handleDelete}>
        <Icon type="trash" />
      </button>
    </div>
  );
};

const StreamTab = ({
  selected,
  label,
  onSelect,
  showErrorIndicator,
  showSchemaConflictIndicator,
  schemaErrors,
  "data-testid": testId,
}: {
  selected: boolean;
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
    className={classNames(styles.tab, { [styles.selectedTab]: selected })}
    onClick={onSelect}
  >
    <Text>{label}</Text>
    {showErrorIndicator && <Indicator />}
    {showSchemaConflictIndicator && <SchemaConflictIndicator errors={schemaErrors} />}
  </button>
);

const SchemaEditor = ({ streamFieldPath }: { streamFieldPath: StreamPathFn }) => {
  const { formatMessage } = useIntl();
  const analyticsService = useAnalyticsService();
  const autoImportSchemaFieldPath = streamFieldPath("autoImportSchema");
  const autoImportSchema = useBuilderWatch(autoImportSchemaFieldPath);
  const schemaFieldPath = streamFieldPath("schema");
  const schema = useBuilderWatch(schemaFieldPath);
  const testStreamIndex = useBuilderWatch("testStreamIndex");
  const { setValue } = useFormContext();
  const path = streamFieldPath("schema");
  const { errors } = useFormState({ name: path });
  const error = get(errors, path);
  const {
    resolvedManifest: { streams },
    streamRead,
  } = useConnectorBuilderTestRead();

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
        disabled={error && !streamRead.data?.inferred_schema}
        disabledTooltip={formatMessage({ id: "connectorBuilder.autoImportSchema.disabledTooltip" })}
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
              stream_name: streams[testStreamIndex]?.name,
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
            key={schemaFieldPath}
            value={schema || ""}
            language="json"
            automaticLayout
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

import { faTrashCan, faCopy } from "@fortawesome/free-regular-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";
import { useField } from "formik";
import { useMemo, useState } from "react";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import Indicator from "components/Indicator";
import { Button } from "components/ui/Button";
import { CodeEditor } from "components/ui/CodeEditor";
import { Text } from "components/ui/Text";

import { Action, Namespace } from "core/services/analytics";
import { useAnalyticsService } from "core/services/analytics";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import {
  BuilderView,
  useConnectorBuilderFormState,
  useConnectorBuilderTestState,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import { AddStreamButton } from "./AddStreamButton";
import { BuilderCard } from "./BuilderCard";
import { BuilderConfigView } from "./BuilderConfigView";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderTitle } from "./BuilderTitle";
import { ErrorHandlerSection } from "./ErrorHandlerSection";
import { IncrementalSection } from "./IncrementalSection";
import { KeyValueListField } from "./KeyValueListField";
import { getOptionsByManifest } from "./manifestHelpers";
import { PaginationSection } from "./PaginationSection";
import { PartitionSection } from "./PartitionSection";
import styles from "./StreamConfigView.module.scss";
import { TransformationSection } from "./TransformationSection";
import { SchemaConflictIndicator } from "../SchemaConflictIndicator";
import { BuilderStream, isEmptyOrDefault } from "../types";
import { formatJson } from "../utils";

interface StreamConfigViewProps {
  streamNum: number;
  hasMultipleStreams: boolean;
}

export const StreamConfigView: React.FC<StreamConfigViewProps> = React.memo(({ streamNum, hasMultipleStreams }) => {
  const { formatMessage } = useIntl();

  const [selectedTab, setSelectedTab] = useState<"configuration" | "schema">("configuration");
  const streamPath = `streams[${streamNum}]`;
  const streamFieldPath = (fieldPath: string) => `${streamPath}.${fieldPath}`;

  return (
    <BuilderConfigView
      heading={formatMessage({ id: "connectorBuilder.stream" })}
      className={hasMultipleStreams ? styles.multiStreams : undefined}
    >
      {/* Not using intl for the labels and tooltips in this component in order to keep maintainence simple */}
      <BuilderTitle path={streamFieldPath("name")} label="Stream Name" size="md" />
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
              label="Record selector"
              manifestPath="DpathExtractor.properties.field_path"
              optional
            />
            <BuilderField type="array" path={streamFieldPath("primaryKey")} manifestPath="PrimaryKey" optional />
          </BuilderCard>
          <PaginationSection streamFieldPath={streamFieldPath} currentStreamIndex={streamNum} />
          <IncrementalSection streamFieldPath={streamFieldPath} currentStreamIndex={streamNum} />
          <PartitionSection streamFieldPath={streamFieldPath} currentStreamIndex={streamNum} />
          <ErrorHandlerSection streamFieldPath={streamFieldPath} currentStreamIndex={streamNum} />
          <TransformationSection streamFieldPath={streamFieldPath} currentStreamIndex={streamNum} />
          <BuilderCard
            copyConfig={{
              path: "requestOptions",
              currentStreamIndex: streamNum,
              copyFromLabel: formatMessage({ id: "connectorBuilder.copyFromRequestOptionsTitle" }),
              copyToLabel: formatMessage({ id: "connectorBuilder.copyToRequestOptionsTitle" }),
            }}
          >
            <KeyValueListField
              path={streamFieldPath("requestOptions.requestParameters")}
              manifestPath="HttpRequester.properties.request_parameters"
            />
            <KeyValueListField
              path={streamFieldPath("requestOptions.requestHeaders")}
              manifestPath="HttpRequester.properties.request_headers"
            />
            <KeyValueListField
              path={streamFieldPath("requestOptions.requestBody")}
              manifestPath="HttpRequester.properties.request_body_json"
            />
          </BuilderCard>
        </>
      ) : (
        <BuilderCard className={styles.schemaEditor}>
          <SchemaEditor streamFieldPath={streamFieldPath} />
        </BuilderCard>
      )}
    </BuilderConfigView>
  );
});

const StreamControls = ({
  streamNum,
  selectedTab,
  setSelectedTab,
  streamFieldPath,
}: {
  streamNum: number;
  streamFieldPath: (path: string) => string;
  setSelectedTab: (tab: "configuration" | "schema") => void;
  selectedTab: "configuration" | "schema";
}) => {
  const analyticsService = useAnalyticsService();
  const { formatMessage } = useIntl();
  const [field, , helpers] = useField<BuilderStream[]>("streams");
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { setSelectedView } = useConnectorBuilderFormState();
  const { streamRead: readStream } = useConnectorBuilderTestState();
  const [schema, meta] = useField<string | undefined>(streamFieldPath("schema"));
  const formattedDetectedSchema = useMemo(
    () => readStream.data?.inferred_schema && formatJson(readStream.data?.inferred_schema, true),
    [readStream.data?.inferred_schema]
  );
  const hasSchemaErrors = Boolean(meta.error);

  const handleDelete = () => {
    openConfirmationModal({
      text: "connectorBuilder.deleteStreamModal.text",
      title: "connectorBuilder.deleteStreamModal.title",
      submitButtonText: "connectorBuilder.deleteStreamModal.submitButton",
      onSubmit: () => {
        const updatedStreams = field.value.filter((_, index) => index !== streamNum);
        const streamToSelect = streamNum >= updatedStreams.length ? updatedStreams.length - 1 : streamNum;
        const viewToSelect: BuilderView = updatedStreams.length === 0 ? "global" : streamToSelect;
        helpers.setValue(updatedStreams);
        setSelectedView(viewToSelect);
        closeConfirmationModal();
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_DELETE, {
          actionDescription: "New stream created from the Add Stream button",
          stream_id: field.value[streamNum].id,
          stream_name: field.value[streamNum].name,
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
        showSchemaConflictIndicator={Boolean(formattedDetectedSchema && schema.value !== formattedDetectedSchema)}
      />
      <AddStreamButton
        onAddStream={(addedStreamNum) => {
          setSelectedView(addedStreamNum);
        }}
        initialValues={field.value[streamNum]}
        button={
          <button className={styles.controlButton} type="button">
            <FontAwesomeIcon icon={faCopy} />
          </button>
        }
        modalTitle={formatMessage(
          { id: "connectorBuilder.copyStreamModal.title" },
          { name: field.value[streamNum].name }
        )}
      />
      <button className={classNames(styles.deleteButton, styles.controlButton)} type="button" onClick={handleDelete}>
        <FontAwesomeIcon icon={faTrashCan} />
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
  "data-testid": testId,
}: {
  selected: boolean;
  label: string;
  onSelect: () => void;
  showErrorIndicator?: boolean;
  showSchemaConflictIndicator?: boolean;
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
    {showSchemaConflictIndicator && <SchemaConflictIndicator />}
  </button>
);

const SchemaEditor = ({ streamFieldPath }: { streamFieldPath: (fieldPath: string) => string }) => {
  const analyticsService = useAnalyticsService();
  const schemaFieldPath = streamFieldPath("schema");
  const [field, meta, helpers] = useField<string | undefined>(schemaFieldPath);
  const { streamRead, streams, testStreamIndex } = useConnectorBuilderTestState();

  const showImportButton = isEmptyOrDefault(field.value) && streamRead.data?.inferred_schema;

  return (
    <>
      {showImportButton && (
        <Button
          full
          variant="secondary"
          onClick={() => {
            const formattedJson = formatJson(streamRead.data?.inferred_schema, true);
            helpers.setValue(formattedJson);
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.OVERWRITE_SCHEMA, {
              actionDescription: "Declared schema overwritten by detected schema",
              stream_name: streams[testStreamIndex]?.name,
            });
          }}
        >
          <FormattedMessage id="connectorBuilder.useSchemaButton" />
        </Button>
      )}
      <div className={styles.editorContainer}>
        <CodeEditor
          key={schemaFieldPath}
          value={field.value || ""}
          language="json"
          theme="airbyte-light"
          onChange={(val: string | undefined) => {
            helpers.setValue(val);
          }}
        />
      </div>
      {meta.error && (
        <Text className={styles.errorMessage}>
          <FormattedMessage id={meta.error} />
        </Text>
      )}
    </>
  );
};

import { useMemo, useEffect } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { SchemaFormControl } from "components/forms/SchemaForm/Controls/SchemaFormControl";
import { SchemaForm } from "components/forms/SchemaForm/SchemaForm";
import { AirbyteJsonSchema } from "components/forms/SchemaForm/utils";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import {
  ConnectorManifest,
  DeclarativeComponentSchema,
  DeclarativeComponentSchemaStreamsItem,
} from "core/api/types/ConnectorManifest";
import { BuilderView, useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./SchemaFormBuilder.module.scss";
import declarativeComponentSchema from "../../../../build/declarative_component_schema.yaml";
import { ViewSelectButton } from "../Builder/BuilderSidebar";
import { Sidebar } from "../Sidebar";
import { DEFAULT_JSON_MANIFEST_STREAM_WITH_URL_BASE } from "../types";
import { useBuilderErrors } from "../useBuilderErrors";

export const SchemaFormBuilder = () => {
  const view: BuilderView = useWatch({ name: "view" });
  const viewPath = useMemo(() => convertViewToPath(view), [view]);
  const { jsonManifest } = useConnectorBuilderFormState();
  const streams = jsonManifest.streams ?? [];

  return (
    <FlexContainer className={styles.container} direction="row" gap="none">
      <SchemaFormBuilderSidebar />
      <SchemaForm<AirbyteJsonSchema, DeclarativeComponentSchema>
        schema={declarativeComponentSchema}
        nestedUnderPath="manifest"
      >
        <SyncValuesToBuilderState />
        {streams.length > 0 && (
          <FlexContainer direction="column" className={styles.formContainer}>
            <FlexItem alignSelf="flex-end">
              <DeleteStreamButton />
            </FlexItem>
            <Card>{viewPath ? <SchemaFormControl key={viewPath} path={viewPath} /> : null}</Card>
          </FlexContainer>
        )}
      </SchemaForm>
    </FlexContainer>
  );
};

const convertViewToPath = (view: BuilderView) => {
  if (view.type === "stream") {
    return `manifest.streams.${view.index}`;
  }

  if (view.type === "dynamic_stream") {
    return `manifest.dynamic_streams.${view.index}`;
  }

  return null;
};

const DeleteStreamButton = () => {
  const { setValue } = useFormContext();
  const view: BuilderView = useWatch({ name: "view" });
  const streams: DeclarativeComponentSchemaStreamsItem[] = useWatch({ name: "manifest.streams" });
  return (
    <Button
      variant="danger"
      onClick={() => {
        if (typeof view !== "number") {
          return;
        }
        if (view === streams.length - 1) {
          setValue("view", { type: "stream", index: streams.length - 2 });
        }
        setValue(
          "manifest.streams",
          streams.filter((_, index) => index !== view)
        );
      }}
    >
      <FormattedMessage id="connectorBuilder.deleteStreamModal.title" />
    </Button>
  );
};

const SyncValuesToBuilderState = () => {
  const { updateJsonManifest, setFormValuesValid } = useConnectorBuilderFormState();
  const { trigger } = useFormContext();
  const schemaFormValues = useWatch({ name: "manifest" }) as ConnectorManifest;

  useEffect(() => {
    // The validation logic isn't updated until the next render cycle, so wait for that
    // before triggering validation and updating the builder state
    setTimeout(() => {
      trigger().then((isValid) => {
        setFormValuesValid(isValid);
        updateJsonManifest(schemaFormValues);
      });
    }, 0);
  }, [schemaFormValues, setFormValuesValid, trigger, updateJsonManifest]);

  return null;
};

const SchemaFormBuilderSidebar = () => {
  const { setValue } = useFormContext();
  const { hasErrors } = useBuilderErrors();
  const selectedView: BuilderView = useWatch({ name: "view" });
  const { jsonManifest } = useConnectorBuilderFormState();
  const streams = jsonManifest.streams ?? [];

  return (
    <Sidebar yamlSelected={false}>
      <FlexContainer className={styles.streamsHeader} alignItems="center" justifyContent="space-between">
        <FlexContainer alignItems="center" gap="none">
          <Text className={styles.streamsHeading} size="xs" bold>
            <FormattedMessage id="connectorBuilder.streamsHeading" values={{ number: streams.length }} />
          </Text>
          <InfoTooltip placement="top">
            <FormattedMessage id="connectorBuilder.streamTooltip" />
          </InfoTooltip>
        </FlexContainer>
        <Button
          type="button"
          className={styles.addStreamButton}
          onClick={() => {
            setValue("manifest.streams", [...streams, DEFAULT_JSON_MANIFEST_STREAM_WITH_URL_BASE]);
            setValue("view", { type: "stream", index: streams.length });
          }}
          icon="plus"
        />
      </FlexContainer>
      <FlexContainer direction="column" gap="xs">
        {streams.map((stream, index) => (
          <ViewSelectButton
            key={`${stream?.name}-${index}`}
            selected={selectedView.type === "stream" && selectedView.index === index}
            onClick={() => {
              console.log("clicked ViewSelectButton", index);
              setValue("view", { type: "stream", index });
            }}
            showIndicator={hasErrors([{ type: "stream", index }]) ? "error" : undefined}
            data-testid="schema-form-builder-view-select-button"
          >
            {stream?.name && stream?.name?.trim() ? (
              <Text className={styles.streamViewText}>{stream.name}</Text>
            ) : (
              <Text className={styles.emptyStreamViewText}>
                <FormattedMessage id="connectorBuilder.emptyName" />
              </Text>
            )}
          </ViewSelectButton>
        ))}
      </FlexContainer>
    </Sidebar>
  );
};

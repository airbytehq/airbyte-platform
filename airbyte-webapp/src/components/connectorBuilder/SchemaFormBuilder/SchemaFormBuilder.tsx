import { useMemo, useEffect } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { SchemaFormControl } from "components/forms/SchemaForm/Controls/SchemaFormControl";
import { SchemaForm } from "components/forms/SchemaForm/SchemaForm";
import { SchemaFormRemainingFields } from "components/forms/SchemaForm/SchemaFormRemainingFields";
import { AirbyteJsonSchema } from "components/forms/SchemaForm/utils";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
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
  const { jsonManifest } = useConnectorBuilderFormState();
  const streams = jsonManifest.streams ?? [];

  return (
    <FlexContainer className={styles.container} direction="row" gap="none">
      <SchemaFormBuilderSidebar />
      <SchemaForm<AirbyteJsonSchema, DeclarativeComponentSchema>
        schema={declarativeComponentSchema}
        nestedUnderPath="manifest"
        refTargetPath="manifest.definitions.linked"
        onlyShowErrorIfTouched
      >
        <SyncValuesToBuilderState />
        {streams.length > 0 && <StreamForm />}
      </SchemaForm>
    </FlexContainer>
  );
};

const StreamForm = () => {
  const view: BuilderView = useWatch({ name: "view" });
  const viewPath = useMemo(() => convertViewToPath(view), [view]);

  if (!viewPath) {
    return null;
  }

  return (
    <FlexContainer direction="column" className={styles.formContainer} key={viewPath}>
      <FlexContainer direction="row" justifyContent="space-between">
        <SchemaFormControl
          path={`${viewPath}.name`}
          titleOverride={null}
          className={styles.streamNameInput}
          placeholder="Enter stream name"
        />
        <DeleteStreamButton />
      </FlexContainer>
      <Card className={styles.card}>
        <SchemaFormControl path={`${viewPath}.retriever.requester.url`} isRequired />
        <SchemaFormControl path={`${viewPath}.retriever.requester.http_method`} />
        <SchemaFormControl path={`${viewPath}.retriever.decoder`} />
        <SchemaFormControl path={`${viewPath}.retriever.record_selector`} nonAdvancedFields={["extractor"]} />
        <SchemaFormControl path={`${viewPath}.primary_key`} />
      </Card>
      <Card className={styles.card}>
        <SchemaFormControl
          path={`${viewPath}.retriever.requester.authenticator`}
          nonAdvancedFields={[
            "api_token",
            "header",
            "username",
            "password",
            "inject_into",
            "client_id",
            "client_secret",
            "refresh_token",
            "token_refresh_endpoint",
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
          ]}
        />
      </Card>
      <Card className={styles.card}>
        <SchemaFormControl path={`${viewPath}.retriever.requester.request_parameters`} />
        <SchemaFormControl path={`${viewPath}.retriever.requester.request_headers`} />
        <SchemaFormControl path={`${viewPath}.retriever.requester.request_body`} />
      </Card>
      <Card className={styles.card}>
        <SchemaFormControl path={`${viewPath}.retriever.paginator`} />
      </Card>
      <Card className={styles.card}>
        <SchemaFormControl path={`${viewPath}.incremental_sync`} />
      </Card>
      <Card className={styles.card}>
        <SchemaFormControl path={`${viewPath}.retriever.partition_router`} />
      </Card>
      <Card className={styles.card}>
        <SchemaFormControl path={`${viewPath}.retriever.requester.error_handler`} />
      </Card>
      <Card className={styles.card}>
        <SchemaFormControl path={`${viewPath}.transformations`} />
      </Card>
      <Card className={styles.card}>
        <SchemaFormRemainingFields path={`${viewPath}.retriever.requester`} />
        <SchemaFormRemainingFields path={`${viewPath}.retriever`} />
        <SchemaFormRemainingFields path={`${viewPath}`} />
      </Card>
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

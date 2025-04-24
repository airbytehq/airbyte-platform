import cloneDeep from "lodash/cloneDeep";
import merge from "lodash/merge";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useFormContext, useFormState, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { v4 as uuid } from "uuid";
import { z } from "zod";

import {
  AssistErrorFormError,
  parseAssistErrorToFormErrors,
  useBuilderAssistCreateConnectorMutation,
} from "components/connectorBuilder/Builder/Assist/assist";
import { AssistWaiting } from "components/connectorBuilder/Builder/Assist/AssistWaiting";
import {
  DEFAULT_CONNECTOR_NAME,
  DEFAULT_JSON_MANIFEST_STREAM,
  DEFAULT_JSON_MANIFEST_VALUES,
  DEFAULT_JSON_MANIFEST_VALUES_WITH_STREAM,
} from "components/connectorBuilder/types";
import { Form, FormControl } from "components/forms";
import { HeadTitle } from "components/HeadTitle";
import { Badge } from "components/ui/Badge";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { DeclarativeComponentSchema, DeclarativeStream } from "core/api/types/ConnectorManifest";
import {
  clearConnectorChatBuilderStorage,
  CONNECTOR_CHAT_ACTIONS,
  getLaunchBuilderParamsFromStorage,
} from "core/utils/connectorChatBuilderStorage";
import { links } from "core/utils/links";
import { convertSnakeToCamel } from "core/utils/strings";
import { useDebounceValue } from "core/utils/useDebounceValue";
import { ToZodSchema } from "core/utils/zod";
import { useExperiment } from "hooks/services/Experiment";
import { ConnectorBuilderLocalStorageProvider } from "services/connectorBuilder/ConnectorBuilderLocalStorageService";
import { ConnectorBuilderFormManagementStateProvider } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./ConnectorBuilderGeneratePage.module.scss";
import { AirbyteTitle } from "../components/AirbyteTitle";
import { BackButton } from "../components/BackButton";
import { useCreateAndNavigate } from "../components/useCreateAndNavigate";

interface GeneratorFormResponse {
  name: string;
  docsUrl?: string;
  openapiSpecUrl?: string;
  firstStream: string;
}

const ConnectorBuilderGeneratePageInner: React.FC = () => {
  const assistSessionId = useMemo(() => uuid(), []);
  const { createAndNavigate, isLoading: isCreateLoading } = useCreateAndNavigate();
  const {
    mutateAsync: getAssistValues,
    isLoading: isAssistLoading,
    error: assistError,
  } = useBuilderAssistCreateConnectorMutation();

  // Ensure we don't show the loading spinner too early
  const isLoading = isCreateLoading || isAssistLoading;
  const debounceTime = isLoading ? 500 : 0;
  const isLoadingWithDelay = useDebounceValue(isCreateLoading || isAssistLoading, debounceTime);

  // These are stored to ensure we persist form values even if the user skips the assist
  const [submittedAssistValues, setSubmittedAssistValues] = useState<GeneratorFormResponse | null>(null);
  const projectName = submittedAssistValues?.name || DEFAULT_CONNECTOR_NAME;

  // Process the error from the assist mutation
  const assistApiErrors = useMemo(() => {
    return parseAssistErrorToFormErrors(assistError);
  }, [assistError]);

  const onCancel = useCallback(() => {
    createAndNavigate({ name: projectName, assistSessionId: undefined });
  }, [createAndNavigate, projectName]);

  const isSchemaFormEnabled = useExperiment("connectorBuilder.schemaForm");

  const onSkip = useCallback(() => {
    const manifest: DeclarativeComponentSchema = isSchemaFormEnabled
      ? cloneDeep(DEFAULT_JSON_MANIFEST_VALUES_WITH_STREAM)
      : cloneDeep(DEFAULT_JSON_MANIFEST_VALUES);
    console.log("manifest", manifest);
    if (!manifest.metadata) {
      manifest.metadata = {};
    }
    manifest.metadata.assist = {
      docsUrl: submittedAssistValues?.docsUrl,
      openapiSpecUrl: submittedAssistValues?.openapiSpecUrl,
    };
    const stream: DeclarativeStream = merge({}, DEFAULT_JSON_MANIFEST_STREAM, {
      name: submittedAssistValues?.firstStream,
    });
    manifest.streams = [stream];
    createAndNavigate({ name: projectName, assistSessionId, manifest });
  }, [
    isSchemaFormEnabled,
    submittedAssistValues?.docsUrl,
    submittedAssistValues?.openapiSpecUrl,
    submittedAssistValues?.firstStream,
    createAndNavigate,
    projectName,
    assistSessionId,
  ]);

  const onFormSubmit = useCallback(
    async (values: GeneratorFormResponse) => {
      // Hold on to the values in case the user skips the assist
      setSubmittedAssistValues(values);

      const assistValues = await getAssistValues({
        session_id: assistSessionId,
        app_name: values.name,
        docs_url: values.docsUrl,
        openapi_spec_url: values.openapiSpecUrl,
        stream_name: values.firstStream,
      });

      createAndNavigate({
        name: values.name || DEFAULT_CONNECTOR_NAME,
        manifest: assistValues.connector,
        assistSessionId,
      });
    },
    [getAssistValues, createAndNavigate, setSubmittedAssistValues, assistSessionId]
  );

  const formSchema = z
    .object({
      name: z.string().trim().nonempty("form.empty.error"),
      docsUrl: z.string().trim().optional(),
      openapiSpecUrl: z.string().trim().optional(),
      firstStream: z.string().trim().nonempty("form.empty.error"),
    } satisfies ToZodSchema<GeneratorFormResponse>)
    .superRefine((values, ctx) => {
      const hasDocsUrl = Boolean(values.docsUrl?.trim());
      const hasOpenapiSpecUrl = Boolean(values.openapiSpecUrl?.trim());

      if (!hasDocsUrl && !hasOpenapiSpecUrl) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: "connectorBuilder.assist.config.docsUrl.oneOf.error",
          path: ["docsUrl"],
        });

        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: "connectorBuilder.assist.config.docsUrl.oneOf.error",
          path: ["openapiSpecUrl"],
        });
      }
    });

  const defaultValues = {
    name: "",
    docsUrl: "",
    openapiSpecUrl: "",
    firstStream: "",
  };

  return (
    <FlexContainer direction="column" gap="2xl" className={styles.container}>
      <AirbyteTitle title={<FormattedMessage id="connectorBuilder.generatePage.prompt" />} />
      <Form defaultValues={defaultValues} zodSchema={formSchema} onSubmit={onFormSubmit}>
        {isLoadingWithDelay ? (
          <AssistWaiting onSkip={onSkip} />
        ) : (
          <ConnectorBuilderGenerateForm
            isLoading={isCreateLoading}
            onCancel={onCancel}
            assistApiErrors={assistApiErrors}
            onSubmit={onFormSubmit}
          />
        )}
      </Form>
    </FlexContainer>
  );
};

const GenerateConnectorFormFields: React.FC<{
  assistApiErrors?: AssistErrorFormError[];
  onSubmit: (values: GeneratorFormResponse) => Promise<void>;
}> = ({ assistApiErrors, onSubmit }) => {
  const { formatMessage } = useIntl();
  const { setError, setValue, handleSubmit, trigger } = useFormContext();
  const isConnectorBuilderGenerateFromParamsEnabled = useExperiment("connectorBuilder.generateConnectorFromParams");

  const { touchedFields } = useFormState();
  const docsUrl = useWatch({ name: "docsUrl" });
  const openapiSpecUrl = useWatch({ name: "openapiSpecUrl" });

  // Show any validation errors from the assist as form field errors
  useEffect(() => {
    for (const error of assistApiErrors ?? []) {
      if (error.fieldName && error.errorMessage) {
        setError(convertSnakeToCamel(error.fieldName), {
          message: error.errorMessage,
        });
      }
    }
  }, [setError, assistApiErrors]);

  useEffect(() => {
    const connectorChatBuilderParams = getLaunchBuilderParamsFromStorage();

    if (connectorChatBuilderParams && isConnectorBuilderGenerateFromParamsEnabled) {
      setValue("name", connectorChatBuilderParams.name);
      setValue("docsUrl", connectorChatBuilderParams.documentation_url);
      setValue("firstStream", connectorChatBuilderParams.stream);
      if (connectorChatBuilderParams.submit_form) {
        const submitAndCleanup = async (data: GeneratorFormResponse) => {
          await onSubmit(data);

          clearConnectorChatBuilderStorage(CONNECTOR_CHAT_ACTIONS.LAUNCH_BUILDER);
        };

        handleSubmit((data) => submitAndCleanup(data as GeneratorFormResponse))();
      } else {
        clearConnectorChatBuilderStorage(CONNECTOR_CHAT_ACTIONS.LAUNCH_BUILDER);
      }
    }
  }, [setValue, handleSubmit, onSubmit, isConnectorBuilderGenerateFromParamsEnabled]);

  useEffect(() => {
    if (touchedFields.docsUrl) {
      trigger("docsUrl");
    }
    if (touchedFields.openapiSpecUrl) {
      trigger("openapiSpecUrl");
    }
  }, [docsUrl, openapiSpecUrl, trigger, touchedFields]);

  return (
    <>
      <FormControl
        fieldType="input"
        name="name"
        type="string"
        label={formatMessage({ id: "connectorBuilder.generatePage.nameLabel" })}
        placeholder={formatMessage({ id: "connectorBuilder.generatePage.namePlaceholder" })}
      />
      <FormControl
        fieldType="input"
        name="docsUrl"
        type="string"
        label={formatMessage({ id: "connectorBuilder.assist.config.docsUrl.label" })}
        placeholder={formatMessage({ id: "connectorBuilder.assist.config.docsUrl.placeholder" })}
        labelTooltip={formatMessage({ id: "connectorBuilder.assist.config.docsUrl.tooltip" })}
      />
      <FormControl
        fieldType="input"
        name="openapiSpecUrl"
        type="string"
        label={formatMessage({ id: "connectorBuilder.assist.config.openapiSpecUrl.label" })}
        placeholder={formatMessage({ id: "connectorBuilder.assist.config.openapiSpecUrl.placeholder" })}
        labelTooltip={formatMessage({ id: "connectorBuilder.assist.config.openapiSpecUrl.tooltip" })}
      />
      <FormControl
        fieldType="input"
        name="firstStream"
        type="string"
        label={formatMessage({ id: "connectorBuilder.generatePage.firstStreamLabel" })}
        placeholder={formatMessage({ id: "connectorBuilder.generatePage.firstStreamPlaceholder" })}
        labelTooltip={formatMessage({ id: "connectorBuilder.generatePage.firstStreamTooltip" })}
      />
    </>
  );
};

const ConnectorBuilderGenerateForm: React.FC<{
  isLoading: boolean;
  assistApiErrors?: AssistErrorFormError[];
  onCancel: () => void;
  onSubmit: (values: GeneratorFormResponse) => Promise<void>;
}> = ({ isLoading, onCancel, assistApiErrors, onSubmit }) => {
  return (
    <FlexContainer direction="column" gap="xl">
      <Card className={styles.formCard} noPadding>
        <FlexContainer className={styles.form} direction="column" gap="lg">
          <FlexContainer direction="row" alignItems="center" gap="sm">
            <Icon type="aiStars" color="magic" size="md" />
            <Heading as="h3" size="sm" className={styles.assistTitle}>
              <FormattedMessage id="connectorBuilder.generatePage.title" />
            </Heading>
            <Badge variant="blue">
              <FormattedMessage id="ui.badge.beta" />
            </Badge>
          </FlexContainer>
          <Text size="sm" color="grey">
            <FormattedMessage id="connectorBuilder.generatePage.description" />
          </Text>
          <Text size="sm" color="grey">
            <FormattedMessage
              id="connectorBuilder.generatePage.description.docsLink"
              values={{
                lnk: (children: React.ReactNode) => (
                  <ExternalLink href={links.connectorBuilderAssist}>{children}</ExternalLink>
                ),
              }}
            />
          </Text>
          <FlexContainer direction="column" gap="none" className={styles.formFields}>
            <GenerateConnectorFormFields assistApiErrors={assistApiErrors} onSubmit={onSubmit} />
          </FlexContainer>
        </FlexContainer>
      </Card>
      <FlexContainer direction="row" alignItems="center" justifyContent="space-between">
        <Button disabled={isLoading} isLoading={isLoading} variant="secondary" type="reset" onClick={onCancel}>
          <FormattedMessage id="connectorBuilder.generatePage.skipLabel" />
        </Button>
        <Button disabled={isLoading} isLoading={isLoading} type="submit">
          <FormattedMessage id="form.create" />
        </Button>
      </FlexContainer>
    </FlexContainer>
  );
};

export const ConnectorBuilderGeneratePage: React.FC = () => (
  <ConnectorBuilderLocalStorageProvider>
    <HeadTitle titles={[{ id: "connectorBuilder.title" }]} />
    <BackButton />
    <ConnectorBuilderFormManagementStateProvider>
      <ConnectorBuilderGeneratePageInner />
    </ConnectorBuilderFormManagementStateProvider>
  </ConnectorBuilderLocalStorageProvider>
);

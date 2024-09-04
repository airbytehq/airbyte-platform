import cloneDeep from "lodash/cloneDeep";
import merge from "lodash/merge";
import { useCallback, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { AssistWaiting } from "components/connectorBuilder/Builder/Assist/AssistWaiting";
import {
  DEFAULT_CONNECTOR_NAME,
  DEFAULT_JSON_MANIFEST_STREAM,
  DEFAULT_JSON_MANIFEST_VALUES,
} from "components/connectorBuilder/types";
import { Form, FormControl } from "components/forms";
import { HeadTitle } from "components/HeadTitle";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useBuilderAssistCreateConnectorMutation } from "core/api";
import { DeclarativeComponentSchema, DeclarativeStream } from "core/api/types/ConnectorManifest";
import { ConnectorBuilderLocalStorageProvider } from "services/connectorBuilder/ConnectorBuilderLocalStorageService";
import { ConnectorBuilderFormManagementStateProvider } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./ConnectorBuilderGeneratePage.module.scss";
import { AirbyteTitle } from "../components/AirbyteTitle";
import { BackButton } from "../components/BackButton";
import { useCreateAndNavigate } from "../components/useCreateAndNavigate";

interface GeneratorFormResponse {
  name: string;
  docsUrl?: string;
  openApiSpecUrl?: string;
  firstStream: string;
}

const ConnectorBuilderGeneratePageInner: React.FC = () => {
  const { createAndNavigate, isLoading } = useCreateAndNavigate();
  const { mutateAsync: getAssistValues, isLoading: isAssistLoading } = useBuilderAssistCreateConnectorMutation();

  // These are stored to ensure we persist form values even if the user skips the assist
  const [submittedAssistValues, setSubmittedAssistValues] = useState<GeneratorFormResponse | null>(null);
  const projectName = submittedAssistValues?.name || DEFAULT_CONNECTOR_NAME;

  const onCancel = useCallback(() => {
    createAndNavigate({ name: projectName, assistEnabled: false });
  }, [createAndNavigate, projectName]);

  const onSkip = useCallback(() => {
    const manifest: DeclarativeComponentSchema = cloneDeep(DEFAULT_JSON_MANIFEST_VALUES);
    if (!manifest.metadata) {
      manifest.metadata = {};
    }
    manifest.metadata.assist = {
      docsUrl: submittedAssistValues?.docsUrl,
      openApiSpecUrl: submittedAssistValues?.openApiSpecUrl,
    };
    const stream: DeclarativeStream = merge({}, DEFAULT_JSON_MANIFEST_STREAM, {
      name: submittedAssistValues?.firstStream,
    });
    manifest.streams = [stream];
    createAndNavigate({ name: projectName, assistEnabled: true, manifest });
  }, [createAndNavigate, submittedAssistValues, projectName]);

  const onFormSubmit = useCallback(
    async (values: GeneratorFormResponse) => {
      // Hold on to the values in case the user skips the assist
      setSubmittedAssistValues(values);

      const assistValues = await getAssistValues({
        app_name: values.name,
        docs_url: values.docsUrl,
        openapi_spec_url: values.openApiSpecUrl,
        stream_name: values.firstStream,
      });

      createAndNavigate({
        name: values.name || DEFAULT_CONNECTOR_NAME,
        manifest: assistValues.connector,
        assistEnabled: true,
      });
    },
    [getAssistValues, createAndNavigate, setSubmittedAssistValues]
  );

  return (
    <FlexContainer direction="column" gap="2xl" className={styles.container}>
      <AirbyteTitle title={<FormattedMessage id="connectorBuilder.generatePage.prompt" />} />
      {isAssistLoading ? (
        <AssistWaiting onSkip={onSkip} />
      ) : (
        <ConnectorBuilderGenerateForm isLoading={isLoading} onSubmit={onFormSubmit} onCancel={onCancel} />
      )}
    </FlexContainer>
  );
};

const ConnectorBuilderGenerateForm: React.FC<{
  isLoading: boolean;
  onSubmit: (values: GeneratorFormResponse) => Promise<void>;
  onCancel: () => void;
}> = ({ isLoading, onSubmit, onCancel }) => {
  const { formatMessage } = useIntl();

  const formSchema = yup.object().shape({
    name: yup.string().required("form.empty.error"),
    docsUrl: yup
      .string()
      .test("oneOfDocsOrOpenApi", "connectorBuilder.assist.config.docsUrl.oneOf.error", (value, context) => {
        const { openApiSpecUrl } = context.parent;
        return Boolean(value?.trim()) || Boolean(openApiSpecUrl?.trim());
      }),
    openApiSpecUrl: yup.string(),
    firstStream: yup.string().required("form.empty.error"),
  });

  const defaultValues = {
    name: "",
    docsUrl: "",
    openApiSpecUrl: "",
    firstStream: "",
  };

  return (
    <Form defaultValues={defaultValues} schema={formSchema} onSubmit={onSubmit}>
      <FlexContainer direction="column" gap="xl">
        <Card className={styles.formCard} noPadding>
          <FlexContainer className={styles.form} direction="column" gap="lg">
            <FlexContainer direction="row" alignItems="center" gap="sm">
              <Icon type="aiStars" color="magic" size="md" />
              <Heading as="h3" size="sm" className={styles.assistTitle}>
                {formatMessage({ id: "connectorBuilder.generatePage.title" })}
              </Heading>
            </FlexContainer>
            <Text size="sm" color="grey">
              <FormattedMessage id="connectorBuilder.generatePage.description" />
            </Text>
            <FlexContainer direction="column" gap="none" className={styles.formFields}>
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
                name="openApiSpecUrl"
                type="string"
                label={formatMessage({ id: "connectorBuilder.assist.config.openApiSpecUrl.label" })}
                placeholder={formatMessage({ id: "connectorBuilder.assist.config.openApiSpecUrl.placeholder" })}
                labelTooltip={formatMessage({ id: "connectorBuilder.assist.config.openApiSpecUrl.tooltip" })}
                optional
              />
              <FormControl
                fieldType="input"
                name="firstStream"
                type="string"
                label={formatMessage({ id: "connectorBuilder.generatePage.firstStreamLabel" })}
                placeholder={formatMessage({ id: "connectorBuilder.generatePage.firstStreamPlaceholder" })}
                labelTooltip={formatMessage({ id: "connectorBuilder.generatePage.firstStreamTooltip" })}
              />
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
    </Form>
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

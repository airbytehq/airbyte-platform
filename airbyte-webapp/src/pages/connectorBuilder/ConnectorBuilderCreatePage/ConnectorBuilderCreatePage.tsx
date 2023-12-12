import { load, YAMLException } from "js-yaml";
import lowerCase from "lodash/lowerCase";
import startCase from "lodash/startCase";
import React, { useCallback, useEffect, useRef, useState } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { HeadTitle } from "components/common/HeadTitle";
import { BuilderFormValues, DEFAULT_CONNECTOR_NAME } from "components/connectorBuilder/types";
import { useManifestToBuilderForm } from "components/connectorBuilder/useManifestToBuilderForm";
import { Button, ButtonProps } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useListBuilderProjects } from "core/api";
import { ConnectorManifest } from "core/api/types/ConnectorManifest";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { links } from "core/utils/links";
import { useNotificationService } from "hooks/services/Notification";
import { ConnectorBuilderLocalStorageProvider } from "services/connectorBuilder/ConnectorBuilderLocalStorageService";

import styles from "./ConnectorBuilderCreatePage.module.scss";
import ImportYamlImage from "./import-yaml.svg?react";
import LoadExistingConnectorImage from "./load-existing-connector.svg?react";
import StartFromScratchImage from "./start-from-scratch.svg?react";
import { AirbyteTitle } from "../components/AirbyteTitle";
import { BackButton } from "../components/BackButton";
import { useCreateAndNavigate } from "../components/useCreateAndNavigate";
import { ConnectorBuilderRoutePaths } from "../ConnectorBuilderRoutes";

const YAML_UPLOAD_ERROR_ID = "connectorBuilder.yamlUpload.error";

const ConnectorBuilderCreatePageInner: React.FC = () => {
  const analyticsService = useAnalyticsService();
  const existingProjects = useListBuilderProjects();
  const [activeTile, setActiveTile] = useState<"yaml" | "empty" | undefined>();
  const navigate = useNavigate();

  const fileInputRef = useRef<HTMLInputElement>(null);
  const { createAndNavigate, isLoading: isCreateProjectLoading } = useCreateAndNavigate();
  const { registerNotification, unregisterNotificationById } = useNotificationService();
  const { convertToBuilderFormValues } = useManifestToBuilderForm();
  const [importYamlLoading, setImportYamlLoading] = useState(false);

  useEffect(() => {
    analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.CONNECTOR_BUILDER_START, {
      actionDescription: "Connector Builder UI create page opened",
    });
  }, [analyticsService]);

  const handleYamlUpload = useCallback(
    async (uploadEvent: React.ChangeEvent<HTMLInputElement>) => {
      setImportYamlLoading(true);
      const file = uploadEvent.target.files?.[0];
      const reader = new FileReader();
      reader.onload = async (readerEvent) => {
        const yaml = readerEvent.target?.result as string;
        const fileName = file?.name;

        try {
          let json;
          try {
            json = load(yaml) as ConnectorManifest;
          } catch (e) {
            if (e instanceof YAMLException) {
              registerNotification({
                id: YAML_UPLOAD_ERROR_ID,
                text: (
                  <FormattedMessage
                    id={YAML_UPLOAD_ERROR_ID}
                    values={{
                      reason: e.reason,
                      line: e.mark.line,
                    }}
                  />
                ),
                type: "error",
              });
              analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.INVALID_YAML_UPLOADED, {
                actionDescription: "A file with invalid YAML syntax was uploaded to the Connector Builder create page",
                error_message: e.reason,
              });
            }
            return;
          }

          let convertedFormValues: BuilderFormValues;
          try {
            convertedFormValues = await convertToBuilderFormValues(json, DEFAULT_CONNECTOR_NAME);
          } catch (e) {
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.UI_INCOMPATIBLE_YAML_IMPORTED, {
              actionDescription: "A YAML manifest that's incompatible with the Builder UI was imported",
              error_message: e.message,
            });
            createAndNavigate({ name: getConnectorName(fileName), manifest: json });
            return;
          }

          analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.UI_COMPATIBLE_YAML_IMPORTED, {
            actionDescription: "A YAML manifest that's compatible with the Builder UI was imported",
          });
          createAndNavigate({ name: getConnectorName(fileName, convertedFormValues), manifest: json });
        } finally {
          if (fileInputRef.current) {
            fileInputRef.current.value = "";
          }
          setImportYamlLoading(false);
        }
      };

      if (file) {
        reader.readAsText(file);
      }
    },
    [analyticsService, convertToBuilderFormValues, createAndNavigate, registerNotification]
  );

  // clear out notification on unmount, so it doesn't persist after a redirect
  useEffect(() => {
    return () => {
      unregisterNotificationById(YAML_UPLOAD_ERROR_ID);
    };
  }, [unregisterNotificationById]);

  const isLoading = isCreateProjectLoading || importYamlLoading;

  return (
    <FlexContainer direction="column" alignItems="center" gap="2xl">
      <AirbyteTitle title={<FormattedMessage id="connectorBuilder.createPage.prompt" />} />
      <FlexContainer direction="row" gap="2xl">
        <input type="file" accept=".yml,.yaml" ref={fileInputRef} onChange={handleYamlUpload} hidden />
        <Tile
          image={<ImportYamlImage />}
          title="connectorBuilder.createPage.importYaml.title"
          description="connectorBuilder.createPage.importYaml.description"
          buttonText="connectorBuilder.createPage.importYaml.button"
          buttonProps={{ isLoading: activeTile === "yaml" && isLoading, disabled: isLoading }}
          onClick={() => {
            unregisterNotificationById(YAML_UPLOAD_ERROR_ID);
            setActiveTile("yaml");
            fileInputRef.current?.click();
          }}
          dataTestId="import-yaml"
        />
        {existingProjects.length > 0 && (
          <Tile
            image={<LoadExistingConnectorImage />}
            title="connectorBuilder.createPage.loadExistingConnector.title"
            description="connectorBuilder.createPage.loadExistingConnector.description"
            buttonText="connectorBuilder.createPage.loadExistingConnector.button"
            buttonProps={{ disabled: isLoading }}
            onClick={() => {
              navigate(`../${ConnectorBuilderRoutePaths.Fork}`);
            }}
            dataTestId="load-existing-connector"
          />
        )}
        <Tile
          image={<StartFromScratchImage />}
          title="connectorBuilder.createPage.startFromScratch.title"
          description="connectorBuilder.createPage.startFromScratch.description"
          buttonText="connectorBuilder.createPage.startFromScratch.button"
          buttonProps={{ isLoading: activeTile === "empty" && isLoading, disabled: isLoading }}
          onClick={() => {
            setActiveTile("empty");
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.START_FROM_SCRATCH, {
              actionDescription: "User selected Start From Scratch on the Connector Builder create page",
            });
            createAndNavigate({ name: getConnectorName() });
          }}
          dataTestId="start-from-scratch"
        />
      </FlexContainer>
      <ExternalLink href={links.connectorBuilderTutorial}>
        <FlexContainer alignItems="center" gap="sm">
          <Icon type="docs" />
          <FormattedMessage id="connectorBuilder.createPage.tutorialPrompt" />
        </FlexContainer>
      </ExternalLink>
    </FlexContainer>
  );
};

function getConnectorName(fileName?: string | undefined, formValues?: BuilderFormValues) {
  if (!fileName) {
    return DEFAULT_CONNECTOR_NAME;
  }
  const fileNameNoType = lowerCase(fileName.split(".")[0].trim());
  if (fileNameNoType === "manifest" && formValues) {
    // remove http protocol from beginning of url
    return formValues.global.urlBase.replace(/(^\w+:|^)\/\//, "");
  }
  return startCase(fileNameNoType);
}

export const ConnectorBuilderCreatePage: React.FC = () => {
  const existingProjects = useListBuilderProjects();

  return (
    <ConnectorBuilderLocalStorageProvider>
      <HeadTitle titles={[{ id: "connectorBuilder.title" }]} />
      {existingProjects.length > 0 && <BackButton />}
      <ConnectorBuilderCreatePageInner />
    </ConnectorBuilderLocalStorageProvider>
  );
};

interface TileProps {
  image: React.ReactNode;
  title: string;
  description: string;
  buttonText: string;
  buttonProps?: Partial<ButtonProps>;
  onClick: () => void;
  dataTestId: string;
}

const Tile: React.FC<TileProps> = ({ image, title, description, buttonText, buttonProps, onClick, dataTestId }) => {
  return (
    <Card className={styles.tile}>
      <FlexContainer direction="column" gap="xl" alignItems="center">
        <FlexContainer justifyContent="center" className={styles.tileImage}>
          {image}
        </FlexContainer>
        <FlexContainer direction="column" alignItems="center" gap="md" className={styles.tileText}>
          <Heading as="h2" size="sm" centered>
            <FormattedMessage id={title} />
          </Heading>
          <FlexContainer direction="column" justifyContent="center" className={styles.tileDescription}>
            <Text align="center">
              <FormattedMessage id={description} />
            </Text>
          </FlexContainer>
        </FlexContainer>
        <Button onClick={onClick} {...buttonProps} data-testid={dataTestId}>
          <FlexContainer direction="row" alignItems="center" gap="md" className={styles.tileButton}>
            <Icon type="arrowRight" />
            <FormattedMessage id={buttonText} />
          </FlexContainer>
        </Button>
      </FlexContainer>
    </Card>
  );
};

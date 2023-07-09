import React, { useEffect, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { ControlLabels } from "components";
import { HeadTitle } from "components/common/HeadTitle";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { BuilderProject, useListBuilderProjects, useListBuilderProjectVersions } from "core/api";
import { ConnectorBuilderLocalStorageProvider } from "services/connectorBuilder/ConnectorBuilderLocalStorageService";

import styles from "./ConnectorBuilderForkPage.module.scss";
import { AirbyteTitle } from "../components/AirbyteTitle";
import { BackButton } from "../components/BackButton";
import { useCreateAndNavigate } from "../components/useCreateAndNavigate";
import { ConnectorBuilderRoutePaths } from "../ConnectorBuilderRoutes";

const ConnectorBuilderForkPageInner: React.FC = () => {
  const { formatMessage } = useIntl();
  const projects = useListBuilderProjects();
  const { createAndNavigate, isLoading: isCreating } = useCreateAndNavigate();
  const navigate = useNavigate();

  useEffect(() => {
    if (projects.length === 0) {
      navigate(ConnectorBuilderRoutePaths.Create, { replace: true });
    }
  }, [navigate, projects.length]);

  const [selectedProject, setSelectedProject] = useState<BuilderProject>(projects[0]);
  const { data: versions, isLoading: isLoadingVersions } = useListBuilderProjectVersions(selectedProject);
  const [selectedVersion, setSelectedVersion] = useState<"draft" | number>("draft");

  useEffect(() => {
    if (!versions) {
      return;
    }
    setSelectedVersion(selectedProject.hasDraft ? "draft" : versions[0]?.version);
  }, [selectedProject, versions]);

  const versionOptions: Array<{ label: React.ReactNode; value: "draft" | number }> = (versions || []).map(
    ({ version, description }) => {
      return {
        label: (
          <FlexContainer alignItems="baseline">
            <Text size="md" as="span">
              v{version}{" "}
            </Text>
            <Text size="sm" as="span" color="grey">
              {description}
            </Text>
          </FlexContainer>
        ),
        value: version,
      };
    }
  );
  if (selectedProject.hasDraft) {
    versionOptions.unshift({
      label: (
        <Text size="md" as="span">
          <FormattedMessage id="connectorBuilder.draft" />
        </Text>
      ),
      value: "draft",
    });
  }

  const isLoading = isCreating || isLoadingVersions;

  return (
    <FlexContainer direction="column" gap="2xl" className={styles.container}>
      <AirbyteTitle title={<FormattedMessage id="connectorBuilder.forkPage.prompt" />} />
      <FlexContainer direction="column" gap="xl">
        <Card className={styles.form}>
          <FlexContainer direction="column" gap="lg">
            <ControlLabels label="Select a connector">
              <ListBox<BuilderProject>
                options={projects.map((project) => {
                  return { label: <Text size="md">{project.name}</Text>, value: project };
                })}
                onSelect={(selected) => selected && setSelectedProject(selected)}
                selectedValue={selectedProject}
              />
            </ControlLabels>
            {versionOptions.length > 1 && (
              <ControlLabels label="Select a version">
                <ListBox<"draft" | number>
                  options={versionOptions}
                  onSelect={(selected) => selected && setSelectedVersion(selected)}
                  selectedValue={selectedVersion}
                />
              </ControlLabels>
            )}
          </FlexContainer>
        </Card>
        <FlexContainer direction="row-reverse">
          <Button
            disabled={isLoading}
            isLoading={isLoading}
            onClick={() => {
              if (isLoading) {
                return;
              }
              createAndNavigate({
                name: formatMessage({ id: "connectorBuilder.forkPage.copyName" }, { oldName: selectedProject.name }),
                forkProjectId: selectedProject.id,
                version: selectedVersion,
              });
            }}
          >
            <FormattedMessage id="connectorBuilder.forkPage.createLabel" />
          </Button>
        </FlexContainer>
      </FlexContainer>
    </FlexContainer>
  );
};

export const ConnectorBuilderForkPage: React.FC = () => (
  <ConnectorBuilderLocalStorageProvider>
    <HeadTitle titles={[{ id: "connectorBuilder.title" }]} />
    <BackButton />
    <ConnectorBuilderForkPageInner />
  </ConnectorBuilderLocalStorageProvider>
);

import React, { useEffect, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { ControlLabels } from "components";
import { HeadTitle } from "components/common/HeadTitle";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { DropDown } from "components/ui/DropDown";
import { FlexContainer } from "components/ui/Flex";

import { ConnectorBuilderLocalStorageProvider } from "services/connectorBuilder/ConnectorBuilderLocalStorageService";
import { BuilderProject, useListProjects } from "services/connectorBuilder/ConnectorBuilderProjectsService";

import styles from "./ConnectorBuilderForkPage.module.scss";
import { AirbyteTitle } from "../components/AirbyteTitle";
import { BackButton } from "../components/BackButton";
import { useCreateAndNavigate } from "../components/useCreateAndNavigate";
import { ConnectorBuilderRoutePaths } from "../ConnectorBuilderRoutes";

const ConnectorBuilderForkPageInner: React.FC = () => {
  const { formatMessage } = useIntl();
  const projects = useListProjects();
  const { createAndNavigate, isLoading } = useCreateAndNavigate();
  const navigate = useNavigate();

  useEffect(() => {
    if (projects.length === 0) {
      navigate(ConnectorBuilderRoutePaths.Create, { replace: true });
    }
  }, [navigate, projects.length]);

  const [selectedProject, setSelectedProject] = useState<BuilderProject>(projects[0]);

  return (
    <FlexContainer direction="column" gap="2xl" className={styles.container}>
      <AirbyteTitle title={<FormattedMessage id="connectorBuilder.forkPage.prompt" />} />
      <FlexContainer direction="column" gap="xl">
        <Card className={styles.form}>
          <ControlLabels label="Select a connector">
            <DropDown
              options={projects.map((project) => {
                return { label: project.name, value: project };
              })}
              onChange={(selected) => selected && setSelectedProject(selected.value)}
              value={selectedProject}
            />
          </ControlLabels>
        </Card>
        <FlexContainer direction="row-reverse">
          <Button
            isLoading={isLoading}
            onClick={() => {
              if (isLoading) {
                return;
              }
              createAndNavigate({
                name: formatMessage({ id: "connectorBuilder.forkPage.copyName" }, { oldName: selectedProject.name }),
                forkProjectId: selectedProject.id,
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

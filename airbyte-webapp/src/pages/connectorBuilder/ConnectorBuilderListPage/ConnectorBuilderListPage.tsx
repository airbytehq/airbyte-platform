import { faPlus } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { DefaultLogoCatalog } from "components/common/DefaultLogoCatalog";
import { HeadTitle } from "components/common/HeadTitle";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { NextTable } from "components/ui/NextTable";
import { Text } from "components/ui/Text";

import { useListProjects } from "services/connectorBuilder/ConnectorBuilderProjectsService";
import { useCurrentWorkspaceId } from "services/workspaces/WorkspacesService";

import styles from "./ConnectorBuilderListPage.module.scss";
import { ConnectorBuilderRoutePaths } from "../ConnectorBuilderRoutes";

interface Project {
  name: string;
  version: string;
}

export const ConnectorBuilderListPage: React.FC = () => {
  const navigate = useNavigate();
  const workspaceId = useCurrentWorkspaceId();
  const projectsReadList = useListProjects(workspaceId);
  // TODO: set version based on activeDeclarativeManifestVersion once it is added to the API
  const projects = projectsReadList.projects.map((projectDetails) => ({ name: projectDetails.name, version: "draft" }));

  const columnHelper = createColumnHelper<Project>();

  const columns = useMemo(
    () => [
      columnHelper.accessor("name", {
        header: () => <FormattedMessage id="connectorBuilder.listPage.name" />,
        cell: (props) => (
          <FlexContainer alignItems="center">
            {/* TODO: replace with custom logos once available */}
            <DefaultLogoCatalog />
            <Text>{props.cell.getValue()}</Text>
          </FlexContainer>
        ),
      }),
      columnHelper.accessor("version", {
        header: () => <FormattedMessage id="connectorBuilder.listPage.version" />,
        cell: (props) => (
          <Text className={classNames({ [styles.draft]: props.cell.getValue() === "draft" })}>
            {props.cell.getValue()}
          </Text>
        ),
      }),
    ],
    [columnHelper]
  );

  return (
    <>
      <HeadTitle titles={[{ id: "connectorBuilder.title" }]} />
      <FlexContainer direction="column" className={styles.container} gap="lg">
        <FlexContainer direction="row" justifyContent="space-between">
          <Heading as="h1" size="lg">
            <FormattedMessage id="connectorBuilder.listPage.heading" values={{ count: projects.length }} />
          </Heading>
          <Button icon={<FontAwesomeIcon icon={faPlus} />} onClick={() => navigate(ConnectorBuilderRoutePaths.Create)}>
            <FormattedMessage id="connectorBuilder.listPage.newConnector" />
          </Button>
        </FlexContainer>
        <NextTable columns={columns} data={projects} />
      </FlexContainer>
    </>
  );
};

import { faPlus } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { createColumnHelper } from "@tanstack/react-table";
import classNames from "classnames";
import queryString from "query-string";
import { useCallback, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { Navigate, useNavigate } from "react-router-dom";

import { MainPageWithScroll } from "components";
import { DefaultLogoCatalog } from "components/common/DefaultLogoCatalog";
import { HeadTitle } from "components/common/HeadTitle";
import { SortOrderEnum } from "components/EntityTable/types";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { NextTable } from "components/ui/NextTable";
import { PageHeader } from "components/ui/PageHeader";
import { SortableTableHeader } from "components/ui/Table";
import { Text } from "components/ui/Text";

import { useQuery } from "hooks/useQuery";
import { useListProjects } from "services/connectorBuilder/ConnectorBuilderProjectsService";

import styles from "./ConnectorBuilderListPage.module.scss";
import { ConnectorBuilderRoutePaths } from "../ConnectorBuilderRoutes";

interface Project {
  name: string;
  version: string;
}

export const ConnectorBuilderListPage: React.FC = () => {
  const navigate = useNavigate();
  const query = useQuery<{ sortBy?: string; order?: SortOrderEnum }>();
  const projectsReadList = useListProjects();
  // TODO: set version based on activeDeclarativeManifestVersion once it is added to the API
  const projects = projectsReadList.projects.map((projectDetails) => ({ name: projectDetails.name, version: "draft" }));

  const sortBy = query.sortBy || "name";
  const sortOrder = query.order || SortOrderEnum.ASC;

  const onSortClick = useCallback(
    (field: string) => {
      const order =
        sortBy !== field ? SortOrderEnum.ASC : sortOrder === SortOrderEnum.ASC ? SortOrderEnum.DESC : SortOrderEnum.ASC;
      navigate({
        search: queryString.stringify(
          {
            sortBy: field,
            order,
          },
          { skipNull: true }
        ),
      });
    },
    [navigate, sortBy, sortOrder]
  );

  const sortData = useCallback(
    (a, b) => {
      const result = a[sortBy].toLowerCase().localeCompare(b[sortBy].toLowerCase());

      if (sortOrder === SortOrderEnum.DESC) {
        return -1 * result;
      }

      return result;
    },
    [sortBy, sortOrder]
  );

  const sortedProjects = useMemo(() => projects.sort(sortData), [sortData, projects]);

  const columnHelper = createColumnHelper<Project>();

  const columns = useMemo(
    () => [
      columnHelper.accessor("name", {
        header: () => (
          <SortableTableHeader
            onClick={() => onSortClick("name")}
            isActive={sortBy === "name"}
            isAscending={sortOrder === SortOrderEnum.ASC}
          >
            <FormattedMessage id="connectorBuilder.listPage.name" />
          </SortableTableHeader>
        ),
        cell: (props) => (
          <FlexContainer alignItems="center" className={styles.nameCell}>
            {/* TODO: replace with custom logos once available */}
            <DefaultLogoCatalog />
            <Text>{props.cell.getValue()}</Text>
          </FlexContainer>
        ),
      }),
      columnHelper.accessor("version", {
        header: () => <FormattedMessage id="connectorBuilder.listPage.version" />,
        cell: (props) => (
          <Text className={classNames(styles.versionCell, { [styles.draft]: props.cell.getValue() === "draft" })}>
            {props.cell.getValue()}
          </Text>
        ),
      }),
    ],
    [columnHelper, onSortClick, sortBy, sortOrder]
  );

  return projects.length ? (
    <MainPageWithScroll
      headTitle={<HeadTitle titles={[{ id: "connectorBuilder.title" }]} />}
      pageTitle={
        <PageHeader
          title={<FormattedMessage id="connectorBuilder.listPage.heading" values={{ count: projects.length }} />}
          endComponent={
            <Button
              icon={<FontAwesomeIcon icon={faPlus} />}
              onClick={() => navigate(ConnectorBuilderRoutePaths.Create)}
              size="sm"
              data-id="new-custom-connector"
            >
              <FormattedMessage id="connectorBuilder.listPage.newConnector" />
            </Button>
          }
        />
      }
    >
      <NextTable columns={columns} data={sortedProjects} className={styles.table} />
    </MainPageWithScroll>
  ) : (
    <Navigate to={ConnectorBuilderRoutePaths.Create} />
  );
};

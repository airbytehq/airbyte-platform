import { faPlus } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import queryString from "query-string";
import { useCallback, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { Navigate, useNavigate } from "react-router-dom";

import { MainPageWithScroll } from "components";
import { HeadTitle } from "components/common/HeadTitle";
import { ConnectorBuilderProjectTable } from "components/ConnectorBuilderProjectTable";
import { SortOrderEnum } from "components/EntityTable/types";
import { Button } from "components/ui/Button";
import { Heading } from "components/ui/Heading";
import { PageHeader } from "components/ui/PageHeader";

import { useListBuilderProjects } from "core/api";
import { useQuery } from "hooks/useQuery";

import { ConnectorBuilderRoutePaths } from "../ConnectorBuilderRoutes";

export const ConnectorBuilderListPage: React.FC = () => {
  const navigate = useNavigate();
  const query = useQuery<{ sortBy?: string; order?: SortOrderEnum }>();
  const projects = useListBuilderProjects();

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
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (a: any, b: any) => {
      const result = a[sortBy].toLowerCase().localeCompare(b[sortBy].toLowerCase());

      if (sortOrder === SortOrderEnum.DESC) {
        return -1 * result;
      }

      return result;
    },
    [sortBy, sortOrder]
  );

  const sortedProjects = useMemo(() => [...projects].sort(sortData), [sortData, projects]);

  return projects.length ? (
    <MainPageWithScroll
      headTitle={<HeadTitle titles={[{ id: "connectorBuilder.title" }]} />}
      pageTitle={
        <PageHeader
          leftComponent={
            <Heading as="h1" size="lg">
              <FormattedMessage id="connectorBuilder.listPage.title" values={{ count: projects.length }} />
            </Heading>
          }
          endComponent={
            <Button
              icon={<FontAwesomeIcon icon={faPlus} />}
              onClick={() => navigate(ConnectorBuilderRoutePaths.Create)}
              size="sm"
              data-testid="new-custom-connector"
            >
              <FormattedMessage id="connectorBuilder.listPage.newConnector" />
            </Button>
          }
        />
      }
    >
      <ConnectorBuilderProjectTable
        onSortClick={onSortClick}
        sortBy={sortBy}
        sortOrder={sortOrder}
        projects={sortedProjects}
      />
    </MainPageWithScroll>
  ) : (
    <Navigate to={ConnectorBuilderRoutePaths.Create} />
  );
};

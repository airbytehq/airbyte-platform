import React from "react";
import { FormattedMessage } from "react-intl";
import { Navigate, useNavigate } from "react-router-dom";

import { HeadTitle } from "components/common/HeadTitle";
import { MainPageWithScroll } from "components/common/MainPageWithScroll";
import { Button } from "components/ui/Button";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { PageHeader } from "components/ui/PageHeader";

import { useSourceList } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { SourcesTable } from "./SourcesTable";
import { SourcePaths } from "../../routePaths";

const AllSourcesPage: React.FC = () => {
  const navigate = useNavigate();
  const { sources } = useSourceList();
  useTrackPage(PageTrackingCodes.SOURCE_LIST);
  const onCreateSource = () => navigate(`${SourcePaths.SelectSourceNew}`);

  return sources.length ? (
    <MainPageWithScroll
      softScrollEdge={false}
      headTitle={<HeadTitle titles={[{ id: "admin.sources" }]} />}
      pageTitle={
        <PageHeader
          leftComponent={
            <Heading as="h1" size="lg">
              <FormattedMessage id="sidebar.sources" />
            </Heading>
          }
          endComponent={
            <Button icon={<Icon type="plus" />} onClick={onCreateSource} size="sm" data-id="new-source">
              <FormattedMessage id="sources.newSource" />
            </Button>
          }
        />
      }
    >
      <SourcesTable sources={sources} />
    </MainPageWithScroll>
  ) : (
    <Navigate to={SourcePaths.SelectSourceNew} />
  );
};

export default AllSourcesPage;

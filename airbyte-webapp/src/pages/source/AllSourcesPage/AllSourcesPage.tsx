import { faPlus } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React from "react";
import { FormattedMessage } from "react-intl";
import { Navigate, useNavigate } from "react-router-dom";

import { HeadTitle } from "components/common/HeadTitle";
import { MainPageWithScroll } from "components/common/MainPageWithScroll";
import { Button } from "components/ui/Button";
import { Heading } from "components/ui/Heading";
import { PageHeader } from "components/ui/PageHeader";
import { NextPageHeader } from "components/ui/PageHeader/NextPageHeader";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useExperiment } from "hooks/services/Experiment";
import { useSourceList } from "hooks/services/useSourceHook";

import { SourcesTable } from "./SourcesTable";
import { SourcePaths } from "../../routePaths";

const AllSourcesPage: React.FC = () => {
  const navigate = useNavigate();
  const { sources } = useSourceList();
  useTrackPage(PageTrackingCodes.SOURCE_LIST);
  const onCreateSource = () => navigate(`${SourcePaths.SelectSourceNew}`);
  const isNewConnectionFlowEnabled = useExperiment("connection.updatedConnectionFlow", false);

  return sources.length ? (
    <MainPageWithScroll
      softScrollEdge={false}
      headTitle={<HeadTitle titles={[{ id: "admin.sources" }]} />}
      pageTitle={
        !isNewConnectionFlowEnabled ? (
          <PageHeader
            title={<FormattedMessage id="sidebar.sources" />}
            endComponent={
              <Button icon={<FontAwesomeIcon icon={faPlus} />} onClick={onCreateSource} size="sm" data-id="new-source">
                <FormattedMessage id="sources.newSource" />
              </Button>
            }
          />
        ) : (
          <NextPageHeader
            leftComponent={
              <Heading as="h1" size="lg">
                <FormattedMessage id="sidebar.sources" />
              </Heading>
            }
            endComponent={
              <Button icon={<FontAwesomeIcon icon={faPlus} />} onClick={onCreateSource} size="sm" data-id="new-source">
                <FormattedMessage id="sources.newSource" />
              </Button>
            }
          />
        )
      }
    >
      <SourcesTable sources={sources} />
    </MainPageWithScroll>
  ) : (
    <Navigate to={SourcePaths.SelectSourceNew} />
  );
};

export default AllSourcesPage;

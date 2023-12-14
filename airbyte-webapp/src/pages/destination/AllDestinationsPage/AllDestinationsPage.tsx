import React from "react";
import { FormattedMessage } from "react-intl";
import { Navigate, useNavigate } from "react-router-dom";

import { HeadTitle } from "components/common/HeadTitle";
import { MainPageWithScroll } from "components/common/MainPageWithScroll";
import { DestinationsTable } from "components/destination/DestinationsTable";
import { Button } from "components/ui/Button";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { PageHeader } from "components/ui/PageHeader";

import { useDestinationList } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { DestinationPaths } from "../../routePaths";

export const AllDestinationsPage: React.FC = () => {
  const navigate = useNavigate();
  const { destinations } = useDestinationList();
  useTrackPage(PageTrackingCodes.DESTINATION_LIST);

  const onCreateDestination = () => navigate(`${DestinationPaths.SelectDestinationNew}`);

  return destinations.length ? (
    <MainPageWithScroll
      softScrollEdge={false}
      headTitle={<HeadTitle titles={[{ id: "admin.destinations" }]} />}
      pageTitle={
        <PageHeader
          leftComponent={
            <Heading as="h1" size="lg">
              <FormattedMessage id="sidebar.destinations" />
            </Heading>
          }
          endComponent={
            <Button icon={<Icon type="plus" />} onClick={onCreateDestination} size="sm" data-id="new-destination">
              <FormattedMessage id="destinations.newDestination" />
            </Button>
          }
        />
      }
    >
      <DestinationsTable destinations={destinations} />
    </MainPageWithScroll>
  ) : (
    <Navigate to={DestinationPaths.SelectDestinationNew} />
  );
};

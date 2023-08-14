import { faPlus } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React from "react";
import { FormattedMessage } from "react-intl";
import { Navigate, useNavigate } from "react-router-dom";

import { HeadTitle } from "components/common/HeadTitle";
import { MainPageWithScroll } from "components/common/MainPageWithScroll";
import { DestinationsTable } from "components/destination/DestinationsTable";
import { Button } from "components/ui/Button";
import { Heading } from "components/ui/Heading";
import { PageHeader } from "components/ui/PageHeader";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useDestinationList } from "hooks/services/useDestinationHook";

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
            <Button
              icon={<FontAwesomeIcon icon={faPlus} />}
              onClick={onCreateDestination}
              size="sm"
              data-id="new-destination"
            >
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

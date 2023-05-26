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
import { NextPageHeader } from "components/ui/PageHeader/NextPageHeader";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useExperiment } from "hooks/services/Experiment";
import { useDestinationList } from "hooks/services/useDestinationHook";

import { RoutePaths } from "../../routePaths";

export const AllDestinationsPage: React.FC = () => {
  const navigate = useNavigate();
  const { destinations } = useDestinationList();
  useTrackPage(PageTrackingCodes.DESTINATION_LIST);
  const isNewConnectionFlowEnabled = useExperiment("connection.updatedConnectionFlow", false);

  const onCreateDestination = () => navigate(`${RoutePaths.DestinationNew}`);

  return destinations.length ? (
    <MainPageWithScroll
      softScrollEdge={false}
      headTitle={<HeadTitle titles={[{ id: "admin.destinations" }]} />}
      pageTitle={
        isNewConnectionFlowEnabled ? (
          <NextPageHeader
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
        ) : (
          <PageHeader
            title={<FormattedMessage id="admin.destinations" />}
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
        )
      }
    >
      <DestinationsTable destinations={destinations} />
    </MainPageWithScroll>
  ) : (
    <Navigate to={RoutePaths.DestinationNew} />
  );
};

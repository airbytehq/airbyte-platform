import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { PageContainer } from "components/PageContainer";
import { ScrollableContainer } from "components/ScrollableContainer";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useCurrentConnection, useFilters, useGetConnectionEvent } from "core/api";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useModalService } from "hooks/services/Modal";

import { ConnectionTimelineAllEventsList, validateAndMapEvent } from "./ConnectionTimelineAllEventsList";
import { ConnectionTimelineFilters } from "./ConnectionTimelineFilters";
import { openJobLogsModal } from "./JobEventMenu";
import { TimelineFilterValues } from "./utils";

const OneEventItem: React.FC<{ eventId: string; connectionId: string }> = ({ eventId, connectionId }) => {
  const { data: singleEventItem } = useGetConnectionEvent(eventId, connectionId);

  if (!singleEventItem) {
    return null;
  }

  return validateAndMapEvent(singleEventItem);
};

export const ConnectionTimelinePage = () => {
  const [scrollElement, setScrollElement] = useState<HTMLDivElement | null>(null);
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_TIMELINE);
  const connection = useCurrentConnection();
  const { openModal } = useModalService();

  const [filterValues, setFilterValue, resetFilters, filtersAreDefault] = useFilters<TimelineFilterValues>({
    status: "",
    eventCategory: "",
    startDate: "",
    endDate: "",
    eventId: "",
    openLogs: "",
    jobId: "",
    attemptNumber: "",
  });

  if (filterValues.openLogs === "true") {
    const jobIdFromFilter = parseInt(filterValues.jobId ?? "");
    const attemptNumberFromFilter = parseInt(filterValues.attemptNumber ?? "");

    openJobLogsModal({
      openModal,
      jobId: !isNaN(jobIdFromFilter) ? jobIdFromFilter : undefined,
      eventId: filterValues.eventId,
      connectionName: connection.name,
      attemptNumber: !isNaN(attemptNumberFromFilter) ? attemptNumberFromFilter : undefined,
      connectionId: connection.connectionId,
      setFilterValue,
    });
  }

  return (
    <ScrollableContainer ref={setScrollElement}>
      <PageContainer centered>
        <ConnectionSyncContextProvider>
          <Box pb="xl">
            <Card noPadding>
              <Box p="lg">
                <FlexContainer direction="column">
                  <FlexContainer justifyContent="space-between" alignItems="center">
                    <Heading as="h5" size="sm" data-testid="connectionTimelinePageHeader">
                      <FormattedMessage id="connection.timeline" />
                    </Heading>
                  </FlexContainer>
                  <ConnectionTimelineFilters
                    filterValues={filterValues}
                    setFilterValue={setFilterValue}
                    resetFilters={resetFilters}
                    filtersAreDefault={filtersAreDefault}
                  />
                </FlexContainer>
              </Box>
              {filterValues.eventId ? (
                <OneEventItem eventId={filterValues.eventId} connectionId={connection.connectionId} />
              ) : (
                <ConnectionTimelineAllEventsList filterValues={filterValues} scrollElement={scrollElement} />
              )}
            </Card>
          </Box>
        </ConnectionSyncContextProvider>
      </PageContainer>
    </ScrollableContainer>
  );
};

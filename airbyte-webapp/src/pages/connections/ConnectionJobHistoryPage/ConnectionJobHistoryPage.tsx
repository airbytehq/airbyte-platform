import dayjs from "dayjs";
import React from "react";
import { FormattedMessage } from "react-intl";
import { useLocation, useNavigate } from "react-router-dom";

import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { EmptyState } from "components/EmptyState";
import { PageContainer } from "components/PageContainer";
import { ScrollableContainer } from "components/ScrollableContainer";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { ClearFiltersButton } from "components/ui/ClearFiltersButton";
import { RangeDatePicker } from "components/ui/DatePicker";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { ListBox, Option } from "components/ui/ListBox";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Text } from "components/ui/Text";

import { useAttemptLink } from "area/connection/utils/attemptLink";
import { useCurrentConnection, useFilters, useListJobs } from "core/api";
import { JobStatus } from "core/api/types/AirbyteClient";
import {
  getFrequencyFromScheduleData,
  Action,
  Namespace,
  useTrackPage,
  PageTrackingCodes,
  useAnalyticsService,
} from "core/services/analytics";

import styles from "./ConnectionJobHistoryPage.module.scss";
import JobsList from "./JobsList";

const JOB_PAGE_SIZE_INCREMENT = 15;

const END_OF_TODAY = dayjs().endOf("day").toISOString();

type JobStatusFilter = "all" | JobStatus;

interface JobHistoryFilterValues {
  jobStatus: JobStatusFilter;
  startDate: string;
  endDate: string;
}

export const ConnectionJobHistoryPage: React.FC = () => {
  const connection = useCurrentConnection();
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_STATUS);
  const [filterValues, setFilterValue, resetFilters, filtersAreDefault] = useFilters<JobHistoryFilterValues>({
    jobStatus: "all",
    startDate: "",
    endDate: "",
  });
  const analyticsService = useAnalyticsService();
  const navigate = useNavigate();
  const { jobId: linkedJobId } = useAttemptLink();
  const { pathname } = useLocation();
  const { data, isLoading, hasNextPage, fetchNextPage, isFetchingNextPage } = useListJobs(
    {
      configId: connection.connectionId,
      configTypes: ["sync", "reset_connection", "clear", "refresh"],
      includingJobId: linkedJobId ? Number(linkedJobId) : undefined,
      statuses: filterValues.jobStatus === "all" ? undefined : [filterValues.jobStatus],
      updatedAtStart: filterValues.startDate !== "" ? startOfDay(filterValues.startDate) : undefined,
      updatedAtEnd: filterValues.endDate !== "" ? endOfDay(filterValues.endDate) : undefined,
    },
    JOB_PAGE_SIZE_INCREMENT
  );

  const jobs = data?.pages.flatMap((page) => page.data.jobs) ?? [];

  const clearLinkedJob = () => {
    navigate(window.location.pathname, { replace: true });
  };

  const updateJobStatusFilter = (status: JobStatus | "all") => {
    clearLinkedJob();
    setFilterValue("jobStatus", status);
  };

  const updateRangeDateFilter = (dates: [string, string]) => {
    clearLinkedJob();
    setFilterValue("startDate", dates[0]);
    setFilterValue("endDate", dates[1]);
  };

  const onLoadMoreJobs = () => {
    fetchNextPage();
    analyticsService.track(Namespace.CONNECTION, Action.LOAD_MORE_JOBS, {
      actionDescription: "Load more jobs button was clicked",
      connection_id: connection.connectionId,
      connector_source: connection.source?.sourceName,
      connector_source_definition_id: connection.source?.sourceDefinitionId,
      connector_destination: connection.destination?.destinationName,
      connector_destination_definition_id: connection.destination?.destinationDefinitionId,
      frequency: getFrequencyFromScheduleData(connection.scheduleData),
      job_page_size: data?.pages.length ?? 1 * JOB_PAGE_SIZE_INCREMENT,
    });
  };

  const clearFilters = () => {
    clearLinkedJob();
    resetFilters();
  };

  const linkedJobNotFound = linkedJobId && jobs.length === 0;

  return (
    <ScrollableContainer>
      <PageContainer centered>
        <ConnectionSyncContextProvider>
          <Card noPadding>
            <Box p="xl">
              <FlexContainer direction="column">
                <FlexContainer justifyContent="space-between" alignItems="center">
                  <Heading as="h5" size="sm">
                    <FormattedMessage id="connectionForm.jobHistory" />
                  </Heading>
                </FlexContainer>
                <FlexContainer alignItems="center">
                  <ListBox
                    buttonClassName={styles.statusFilter}
                    options={statusFilterOptions}
                    onSelect={(value) => updateJobStatusFilter(value)}
                    selectedValue={filterValues.jobStatus}
                    id="job-history-status-filter"
                  />
                  <RangeDatePicker
                    value={[filterValues.startDate, filterValues.endDate]}
                    onChange={updateRangeDateFilter}
                    maxDate={END_OF_TODAY}
                    buttonText="jobHistory.rangeDateFilter"
                  />
                  {!filtersAreDefault && <ClearFiltersButton onClick={clearFilters} />}
                  <span className={styles.jobCount}>
                    {!isLoading && (
                      <Text color="grey">
                        <FormattedMessage id="jobHistory.count" values={{ count: data?.pages[0].data.totalJobCount }} />
                      </Text>
                    )}
                  </span>
                </FlexContainer>
              </FlexContainer>
            </Box>
            {isLoading ? (
              <Box py="2xl">
                <FlexContainer justifyContent="center">
                  <LoadingSpinner />
                </FlexContainer>
              </Box>
            ) : jobs?.length ? (
              <JobsList jobs={jobs} />
            ) : linkedJobNotFound ? (
              <Box pb="xl">
                <EmptyState
                  text={<FormattedMessage id="connection.linkedJobNotFound" />}
                  description={
                    <Link to={pathname}>
                      <FormattedMessage id="connection.returnToJobHistory" />
                    </Link>
                  }
                />
              </Box>
            ) : (
              <Box pb="xl">
                <EmptyState
                  text={<FormattedMessage id="jobs.noJobs" />}
                  description={
                    <FormattedMessage
                      id={filtersAreDefault ? "jobs.noJobsDescription" : "jobs.noJobsFilterDescription"}
                    />
                  }
                />
              </Box>
            )}
          </Card>
          {hasNextPage && (
            <footer className={styles.footer}>
              <Button variant="secondary" isLoading={isFetchingNextPage} onClick={onLoadMoreJobs}>
                <FormattedMessage id="connection.loadMoreJobs" />
              </Button>
            </footer>
          )}
        </ConnectionSyncContextProvider>
      </PageContainer>
    </ScrollableContainer>
  );
};

function startOfDay(date: string) {
  try {
    return dayjs(date, "YYYY-MM-DD").startOf("day").toISOString();
  } catch {
    return undefined;
  }
}

function endOfDay(date: string) {
  try {
    return dayjs(date, "YYYY-MM-DD").endOf("day").toISOString();
  } catch {
    return undefined;
  }
}

const statusFilterOptions: Array<Option<JobStatus | "all">> = [
  {
    label: (
      <Text color="grey" bold>
        <FormattedMessage id="jobHistory.statusFilter.allJobs" />{" "}
      </Text>
    ),
    value: "all",
  },
  {
    label: <FormattedMessage id="jobHistory.statusFilter.successful" />,
    value: "succeeded",
    icon: <Icon type="successFilled" size="md" color="success" />,
  },
  {
    label: <FormattedMessage id="jobHistory.statusFilter.failed" />,
    value: "failed",
    icon: <Icon type="errorFilled" size="md" color="error" />,
  },
  {
    label: <FormattedMessage id="jobHistory.statusFilter.cancelled" />,
    value: "cancelled",
    icon: <Icon type="statusCancelled" size="md" color="action" />,
  },
];

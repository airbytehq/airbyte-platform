import classNames from "classnames";
import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import { useLocation } from "react-router-dom";

import { EmptyResourceBlock } from "components/common/EmptyResourceBlock";
import { ConnectionSyncButtons } from "components/connection/ConnectionSync/ConnectionSyncButtons";
import { ConnectionSyncContextProvider } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { useAttemptLink } from "components/JobItem/attemptLinkUtils";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { Link } from "components/ui/Link";

import { useListJobs } from "core/api";
import { getFrequencyFromScheduleData } from "core/services/analytics";
import { Action, Namespace } from "core/services/analytics";
import { useTrackPage, PageTrackingCodes, useAnalyticsService } from "core/services/analytics";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useExperiment } from "hooks/services/Experiment";

import styles from "./ConnectionJobHistoryPage.module.scss";
import JobsList from "./JobsList";

const JOB_PAGE_SIZE_INCREMENT = 25;

export const ConnectionJobHistoryPage: React.FC = () => {
  const { connection } = useConnectionEditService();
  useTrackPage(PageTrackingCodes.CONNECTIONS_ITEM_STATUS);
  const [jobPageSize, setJobPageSize] = useState(JOB_PAGE_SIZE_INCREMENT);
  const analyticsService = useAnalyticsService();
  const { jobId: linkedJobId } = useAttemptLink();
  const { pathname } = useLocation();
  const {
    data: { jobs, totalJobCount },
    isPreviousData: isJobPageLoading,
  } = useListJobs({
    configId: connection.connectionId,
    configTypes: ["sync", "reset_connection"],
    includingJobId: linkedJobId ? Number(linkedJobId) : undefined,
    pagination: {
      pageSize: jobPageSize,
    },
  });
  const searchableJobLogsEnabled = useExperiment("connection.searchableJobLogs", true);

  const linkedJobNotFound = linkedJobId && jobs.length === 0;
  const moreJobPagesAvailable = !linkedJobNotFound && jobPageSize < totalJobCount;

  const onLoadMoreJobs = () => {
    setJobPageSize((prevJobPageSize) => prevJobPageSize + JOB_PAGE_SIZE_INCREMENT);

    analyticsService.track(Namespace.CONNECTION, Action.LOAD_MORE_JOBS, {
      actionDescription: "Load more jobs button was clicked",
      connection_id: connection.connectionId,
      connector_source: connection.source?.sourceName,
      connector_source_definition_id: connection.source?.sourceDefinitionId,
      connector_destination: connection.destination?.destinationName,
      connector_destination_definition_id: connection.destination?.destinationDefinitionId,
      frequency: getFrequencyFromScheduleData(connection.scheduleData),
      job_page_size: jobPageSize,
    });
  };

  return (
    <div className={classNames(searchableJobLogsEnabled && styles.narrowTable)}>
      <ConnectionSyncContextProvider>
        <Card
          title={
            <div className={styles.title}>
              <FormattedMessage id="connectionForm.jobHistory" />
              <div className={styles.actions}>
                <ConnectionSyncButtons buttonText={<FormattedMessage id="connection.startSync" />} />
              </div>
            </div>
          }
        >
          {jobs.length ? (
            <JobsList jobs={jobs} />
          ) : linkedJobNotFound ? (
            <EmptyResourceBlock
              text={<FormattedMessage id="connection.linkedJobNotFound" />}
              description={
                <Link to={pathname}>
                  <FormattedMessage id="connection.returnToJobHistory" />
                </Link>
              }
            />
          ) : (
            <EmptyResourceBlock text={<FormattedMessage id="sources.noSync" />} />
          )}
        </Card>
        {(moreJobPagesAvailable || isJobPageLoading) && (
          <footer className={styles.footer}>
            <Button variant="secondary" isLoading={isJobPageLoading} onClick={onLoadMoreJobs}>
              <FormattedMessage id="connection.loadMoreJobs" />
            </Button>
          </footer>
        )}
      </ConnectionSyncContextProvider>
    </div>
  );
};

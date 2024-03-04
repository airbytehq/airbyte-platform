import React from "react";
import { useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { DbtCloudJob } from "core/api/cloud";

import dbtLogo from "./dbt-bit_tm.svg";
import styles from "./JobListItem.module.scss";

interface JobListItemProps {
  job: DbtCloudJob;
  removeJob: () => void;
  isLoading: boolean;
  key?: string;
}

/**
 * react-hook-form version of JobsListItem
 * @param job
 * @param removeJob
 * @param isLoading
 * @param key
 */
export const JobListItem: React.FC<JobListItemProps> = ({
  job,
  removeJob,
  isLoading,
  ...restProps
}: JobListItemProps) => {
  const { formatMessage } = useIntl();
  /**
   * @author: Alex Birdsall
   * TODO: if `job.jobName` is undefined, that means we failed to match any of the
   * dbt-Cloud-supplied jobs with the saved job. This means one of two things has
   * happened:
   * 1) the user deleted the job in dbt Cloud, and we should make them delete it from
   *    their webhook operations. If we have a nonempty list of other dbt Cloud jobs,
   *    it's definitely this.
   * 2) the API call to fetch the names failed somehow (possibly with a 200 status, if there's a bug)
   */
  const title = <Text>{job.jobName || formatMessage({ id: "connection.dbtCloudJobs.job.title" })}</Text>;

  return (
    <Card className={styles.jobListItem} {...restProps}>
      <FlexContainer alignItems="center" className={styles.jobListItem__integrationName}>
        <img src={dbtLogo} alt="" className={styles.jobListItem__dbtLogo} />
        {title}
      </FlexContainer>
      <FlexContainer alignItems="center" justifyContent="space-between" className={styles.jobListItem__idFieldGroup}>
        <FlexItem grow className={styles.jobListItem__idField}>
          <Text size="sm" color="grey600">
            {formatMessage({ id: "connection.dbtCloudJobs.job.accountId" })}: {job.accountId}
          </Text>
        </FlexItem>
        <FlexItem grow className={styles.jobListItem__idField}>
          <Text size="sm" color="grey600">
            {formatMessage({ id: "connection.dbtCloudJobs.job.jobId" })}: {job.jobId}
          </Text>
        </FlexItem>
        <Button
          size="lg"
          type="button"
          variant="clear"
          onClick={removeJob}
          disabled={isLoading}
          icon={<Icon type="cross" />}
          aria-label={formatMessage({ id: "connection.dbtCloudJobs.job.deleteButton" })}
        />
      </FlexContainer>
    </Card>
  );
};

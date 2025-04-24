import classNames from "classnames";
import React from "react";
import { useFieldArray, useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { EmptyState } from "components/EmptyState";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { DropdownMenu } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { isSameJob } from "core/api/cloud";
import { DbtCloudJobInfo } from "core/api/types/AirbyteClient";

import { DbtCloudTransformationsFormValues } from "./DbtCloudTransformationsForm";
import styles from "./DbtCloudTransformationsFormControls.module.scss";
import { NoDbtIntegrationMsg } from "./NoDbtIntegrationMessage";
import { NoJobsFoundForAccountMsg } from "./NoJobsFoundForAccountMessage";
import { JobListItem } from "../JobListItem";

interface DbtCloudTransformationsFormControlsProps {
  availableDbtCloudJobs: DbtCloudJobInfo[];
  hasDbtIntegration: boolean;
}

export const DbtCloudTransformationsFormControls: React.FC<DbtCloudTransformationsFormControlsProps> = ({
  availableDbtCloudJobs,
  hasDbtIntegration,
}) => {
  const { fields, append, remove } = useFieldArray<DbtCloudTransformationsFormValues>({ name: "jobs" });
  const { isSubmitting } = useFormState<DbtCloudTransformationsFormValues>();

  /**
   * get dropdown menu options from available dbt cloud jobs that are not added to the form
   */
  const dropdownMenuOptions = availableDbtCloudJobs
    .filter((remoteJob) => !fields.some((savedJob) => isSameJob(remoteJob, savedJob)))
    .map((job) => ({ displayName: job.jobName, value: job }));

  const isFormFieldsEmpty = fields.length === 0;

  return (
    <>
      <FlexContainer justifyContent={!isFormFieldsEmpty ? "space-between" : "flex-end"} alignItems="center">
        {!isFormFieldsEmpty ? (
          <Text color="grey">
            <FormattedMessage id="connection.dbtCloudJobs.explanation" />
          </Text>
        ) : null}
        {hasDbtIntegration ? (
          availableDbtCloudJobs.length > 0 ? (
            <DropdownMenu
              options={dropdownMenuOptions}
              placement="bottom"
              onChange={(selection) => {
                append(selection.value as DbtCloudJobInfo);
              }}
            >
              {() => (
                <Button variant="secondary" icon="plus" iconSize="sm" disabled={!dropdownMenuOptions.length}>
                  <FormattedMessage id="connection.dbtCloudJobs.addJob" />
                </Button>
              )}
            </DropdownMenu>
          ) : (
            <NoJobsFoundForAccountMsg />
          )
        ) : null}
      </FlexContainer>
      <Box p="md" className={classNames({ [styles.cardBodyContainer]: !isFormFieldsEmpty })}>
        {!isFormFieldsEmpty ? (
          <FlexContainer direction="column" gap="md">
            {fields.map((field, index) => (
              <JobListItem key={field.id} job={field} removeJob={() => remove(index)} isLoading={isSubmitting} />
            ))}
          </FlexContainer>
        ) : (
          <FlexContainer alignItems="center" justifyContent="center">
            <FlexContainer direction="column" alignItems="center">
              {!hasDbtIntegration ? (
                <NoDbtIntegrationMsg />
              ) : isFormFieldsEmpty ? (
                <Box py="lg">
                  <EmptyState icon="folder" text={<FormattedMessage id="connection.dbtCloudJobs.noJobs" />} />
                </Box>
              ) : null}
            </FlexContainer>
          </FlexContainer>
        )}
      </Box>
    </>
  );
};

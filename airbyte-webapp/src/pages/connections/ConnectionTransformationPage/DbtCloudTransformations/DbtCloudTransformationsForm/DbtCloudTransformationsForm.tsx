import React from "react";
import { useIntl } from "react-intl";
import { z } from "zod";

import { Form } from "components/forms";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { useCurrentConnection } from "core/api";
import { DbtCloudJob, isSameJob, useDbtIntegration } from "core/api/cloud";
import { DbtCloudJobInfo } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";
import { ToZodSchema } from "core/utils/zod";
import { useNotificationService } from "hooks/services/Notification";

import { DbtCloudTransformationsFormControls } from "./DbtCloudTransformationsFormControls";

const DbtCloudJobSchema = z.object({
  jobId: z.number(),
  accountId: z.number(),
  operationId: z.string().optional(),
  jobName: z.string().optional(),
} satisfies ToZodSchema<DbtCloudJob>);

const dbtJobsValidationSchema = z.object({
  jobs: z.array(DbtCloudJobSchema),
});

export type DbtCloudTransformationsFormValues = z.infer<typeof dbtJobsValidationSchema>;

interface DbtCloudTransformationsFormProps {
  availableDbtCloudJobs?: DbtCloudJobInfo[];
}

export const DbtCloudTransformationsForm: React.FC<DbtCloudTransformationsFormProps> = ({
  availableDbtCloudJobs = [],
}) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const connection = useCurrentConnection();
  const { hasDbtIntegration, saveJobs, dbtCloudJobs } = useDbtIntegration(connection);

  /**
   *  @author: Alex Birdsall
   *  because we don't store names for saved jobs, just the account and job IDs needed for
   *  webhook operation, we have to find the display names for saved jobs by comparing IDs
   *  with the list of available jobs as provided by dbt Cloud.
   */
  const jobs: DbtCloudJob[] = dbtCloudJobs.map((savedJob) => {
    const { jobName } = availableDbtCloudJobs.find((remoteJob) => isSameJob(remoteJob, savedJob)) || {};
    const { accountId, jobId } = savedJob;

    return { accountId, jobId, jobName };
  });

  const onSubmit = async ({ jobs }: DbtCloudTransformationsFormValues) => {
    await saveJobs(jobs);
  };

  const onSuccess = () => {
    registerNotification({
      id: "dbt_cloud_transformations_update_success",
      text: formatMessage({ id: "connection.dbtCloudJobs.updateSuccess" }),
      type: "success",
    });
  };

  const onError = (e: Error, { jobs }: DbtCloudTransformationsFormValues) => {
    trackError(e, { jobs });

    registerNotification({
      id: "dbt_cloud_transformations_update_error",
      text: formatMessage({
        id: "connection.dbtCloudJobs.updateError",
      }),
      type: "error",
    });
  };

  return (
    <Form<DbtCloudTransformationsFormValues>
      zodSchema={dbtJobsValidationSchema}
      defaultValues={{ jobs: hasDbtIntegration ? jobs : dbtCloudJobs }}
      onSubmit={onSubmit}
      onSuccess={onSuccess}
      onError={onError}
      trackDirtyChanges
    >
      <Card title={formatMessage({ id: "connection.dbtCloudJobs.cardTitle" })} collapsible>
        <FlexContainer direction="column">
          <DbtCloudTransformationsFormControls
            availableDbtCloudJobs={availableDbtCloudJobs}
            hasDbtIntegration={hasDbtIntegration}
          />
          {hasDbtIntegration || jobs.length || dbtCloudJobs.length ? (
            <FormSubmissionButtons submitKey="form.saveChanges" />
          ) : null}
        </FlexContainer>
      </Card>
    </Form>
  );
};

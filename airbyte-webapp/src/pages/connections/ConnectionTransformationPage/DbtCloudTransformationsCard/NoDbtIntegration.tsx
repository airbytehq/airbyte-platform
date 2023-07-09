import classNames from "classnames";
import { FieldArray, Form, Formik, FormikHelpers } from "formik";
import { ReactNode } from "react";
import { FormattedMessage } from "react-intl";

import { FormChangeTracker } from "components/common/FormChangeTracker";
import { Card } from "components/ui/Card";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useDbtIntegration } from "core/api/cloud";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { RoutePaths } from "pages/routePaths";

import { DbtJobListValues } from "./DbtJobsForm";
import { JobsList } from "./JobsList";
import styles from "./NoDbtIntegration.module.scss";

export const NoDbtIntegration = () => {
  const workspaceId = useCurrentWorkspaceId();
  const { connection } = useConnectionEditService();
  const dbtSettingsPath = `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Settings}/dbt-cloud`;
  const { saveJobs } = useDbtIntegration(connection);

  const onSubmit = (values: DbtJobListValues, { resetForm }: FormikHelpers<DbtJobListValues>) => {
    saveJobs(values.jobs).then(() => resetForm({ values }));
  };

  const jobs = connection.operations
    ?.filter((operation) => operation.operatorConfiguration.webhook?.webhookType === "dbtCloud")
    .map((operation) => {
      return { accountId: 0, jobId: 0, operationId: operation.operationId };
    });

  return (
    <Card
      title={
        <span className={styles.cardTitle}>
          <FormattedMessage id="connection.dbtCloudJobs.cardTitle" />
        </span>
      }
    >
      {!jobs?.length ? (
        <div className={classNames(styles.cardBodyContainer)}>
          <Text className={styles.contextExplanation}>
            <FormattedMessage
              id="connection.dbtCloudJobs.noIntegration"
              values={{
                settingsLink: (linkText: ReactNode) => <Link to={dbtSettingsPath}>{linkText}</Link>,
              }}
            />
          </Text>
        </div>
      ) : (
        <Formik
          onSubmit={onSubmit}
          initialValues={{ jobs }}
          render={({ values, dirty }) => {
            return (
              <Form className={styles.jobListForm}>
                <FormChangeTracker changed={dirty} />
                <FieldArray
                  name="jobs"
                  render={({ remove }) => {
                    return <JobsList jobs={values.jobs} remove={remove} dirty={dirty} isLoading={false} />;
                  }}
                />
              </Form>
            );
          }}
        />
      )}
    </Card>
  );
};

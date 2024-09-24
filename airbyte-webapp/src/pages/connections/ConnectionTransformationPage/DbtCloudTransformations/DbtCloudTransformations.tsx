import React from "react";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCurrentConnection } from "core/api";
import { useAvailableDbtJobs, useDbtIntegration } from "core/api/cloud";

import { DbtCloudErrorBoundary } from "./DbtCloudErrorBoundary";
import { DbtCloudTransformationsForm } from "./DbtCloudTransformationsForm";

/**
 * wrapper for DbtCloudTransformationsForm
 * to fetch availableDbtJobs from the API since we can't call hooks conditionally
 */
const DbtCloudTransformationsWithDbtIntegration: React.FC = () => {
  const availableDbtCloudJobs = useAvailableDbtJobs();
  return <DbtCloudTransformationsForm availableDbtCloudJobs={availableDbtCloudJobs} />;
};

/**
 * react-hook-form version of DbtCloudTransformationsCard
 */
export const DbtCloudTransformations: React.FC = () => {
  // Possible render paths:
  // 1) IF the workspace has no dbt cloud account linked
  //    THEN show "go to your settings to connect your dbt Cloud Account" text
  // 2) IF the workspace HAD a dbt cloud account linked previously
  //    THEN show all saved transformations jobs (if they were saved before the account was remove)
  //    ELSE show "go to your settings to connect your dbt Cloud Account" text
  // 2) IF the workspace has a dbt cloud account linked...
  //   2.1) AND the connection has no saved dbt jobs (cf: operations)
  //        THEN show empty jobs list and "No transformations" text
  //   2.2) AND the connection has saved dbt jobs
  //        THEN show the jobs list
  //   2.3) AND there are no available jobs from the dbt cloud API
  //        THEN show empty jobs list and the "No jobs found for this dbt Cloud account" text
  //   2.4) AND there are available jobs from the dbt cloud API
  //        THEN show empty "+ Add transformation" button

  const connection = useCurrentConnection();
  const { hasDbtIntegration } = useDbtIntegration(connection);
  /**
   * we can't use hooks inside the class component, so we to pass them as props
   */
  const workspaceId = useCurrentWorkspaceId();

  return hasDbtIntegration ? (
    <DbtCloudErrorBoundary workspaceId={workspaceId}>
      <DbtCloudTransformationsWithDbtIntegration />
    </DbtCloudErrorBoundary>
  ) : (
    <DbtCloudTransformationsForm />
  );
};

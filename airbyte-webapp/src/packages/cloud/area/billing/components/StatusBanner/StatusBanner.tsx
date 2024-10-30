import React from "react";

import { AlertBanner } from "components/ui/Banner/AlertBanner";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCurrentOrganizationInfo } from "core/api";
import { useGetCloudWorkspaceAsync } from "core/api/cloud";
import { useExperiment } from "hooks/services/Experiment";

import { WorkspaceStatusBanner as LegacyWorkspaceStatusBanner } from "./LegacyStatusBanner/WorkspaceStatusBanner";
import { useBillingStatusBanner } from "../../utils/useBillingStatusBanner";

const LegacyStatusBanner: React.FC = () => {
  const workspaceId = useCurrentWorkspaceId();
  const isBillingMigrationMaintenance = useExperiment("billing.migrationMaintenance");
  const cloudWorkspace = useGetCloudWorkspaceAsync(workspaceId);
  return cloudWorkspace && !isBillingMigrationMaintenance ? (
    <LegacyWorkspaceStatusBanner cloudWorkspace={cloudWorkspace} />
  ) : null;
};

const WorkspaceStatusBanner: React.FC = () => {
  const statusBanner = useBillingStatusBanner("top_level");
  return statusBanner ? (
    <AlertBanner data-testid="billing-status-banner" message={statusBanner.content} color={statusBanner.level} />
  ) : null;
};

export const StatusBanner: React.FC = () => {
  const { billing } = useCurrentOrganizationInfo();
  return <React.Suspense>{billing ? <WorkspaceStatusBanner /> : <LegacyStatusBanner />}</React.Suspense>;
};

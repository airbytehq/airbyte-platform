import { useMemo } from "react";

import { WorkspaceRead } from "core/api/types/AirbyteClient";
import { useAnalyticsRegisterValues } from "core/services/analytics/useAnalyticsService";
import { useExperimentContext } from "core/services/Experiment";

/** Adds the workspace to the experiment context and to every tracked event, until the caller unmounts. */
export const useRegisterWorkspaceContext = (workspace: WorkspaceRead) => {
  useExperimentContext("workspace", workspace.workspaceId);

  const analyticsContext = useMemo(
    () => ({
      workspace_id: workspace.workspaceId,
      customer_id: workspace.customerId,
      organization_id: workspace.organizationId,
    }),
    [workspace]
  );
  useAnalyticsRegisterValues(analyticsContext);
};

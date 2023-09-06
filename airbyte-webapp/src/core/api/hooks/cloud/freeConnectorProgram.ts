import { useQuery } from "@tanstack/react-query";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { FeatureItem, useFeature } from "core/services/features";
import { useExperiment } from "hooks/services/Experiment";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";

import { webBackendGetFreeConnectorProgramInfoForWorkspace } from "../../generated/CloudApi";

/**
 * @deprecated this hook will be removed after sunsetting the fcp
 *
 * do not use
 */

export const useFreeConnectorProgram = () => {
  const workspaceId = useCurrentWorkspaceId();
  const middlewares = useDefaultRequestMiddlewares();
  const requestOptions = { middlewares };
  const freeConnectorProgramEnabled = useFeature(FeatureItem.FreeConnectorProgram);
  const fcpSunsetPlatform = useExperiment("platform.sunset-fcp", false);

  if (fcpSunsetPlatform) {
    throw new Error("FCP is sunset on platform");
  }

  const programStatusQuery = useQuery(["freeConnectorProgramInfo", workspaceId], () =>
    webBackendGetFreeConnectorProgramInfoForWorkspace({ workspaceId }, requestOptions).then(
      ({ hasPaymentAccountSaved, hasEligibleConnections, hasNonEligibleConnections }) => {
        return {
          isEnrolled: freeConnectorProgramEnabled && hasPaymentAccountSaved,
          hasEligibleConnections: freeConnectorProgramEnabled && hasEligibleConnections,
          hasNonEligibleConnections: freeConnectorProgramEnabled && hasNonEligibleConnections,
        };
      }
    )
  );

  return {
    programStatusQuery,
  };
};

export const useIsFCPEnabled = () => {
  const frontendFCPEnabled = useFeature(FeatureItem.FreeConnectorProgram);
  const platformFCPSunset = useExperiment("platform.sunset-fcp", false);
  return frontendFCPEnabled && !platformFCPSunset;
};

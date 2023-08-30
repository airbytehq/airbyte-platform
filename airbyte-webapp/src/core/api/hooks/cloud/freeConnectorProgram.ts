import { useQuery } from "@tanstack/react-query";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { FeatureItem, useFeature } from "core/services/features";
import { useDefaultRequestMiddlewares } from "services/useDefaultRequestMiddlewares";

import { webBackendGetFreeConnectorProgramInfoForWorkspace } from "../../generated/CloudApi";

export const useFreeConnectorProgram = () => {
  const workspaceId = useCurrentWorkspaceId();
  const middlewares = useDefaultRequestMiddlewares();
  const requestOptions = { middlewares };
  const freeConnectorProgramEnabled = useFeature(FeatureItem.FreeConnectorProgram);

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

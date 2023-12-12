import { Navigate } from "react-router-dom";

import { useListWorkspacesInfinite } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { RoutePaths } from "pages/routePaths";

export const DefaultView: React.FC = () => {
  const { data: workspacesData } = useListWorkspacesInfinite(2, "", true);
  const workspaces = workspacesData?.pages.flatMap((page) => page.data.workspaces) ?? [];
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);

  // Only show the workspace list if there is not exactly one workspace
  // otherwise redirect to the single workspace

  return (
    <Navigate
      to={
        workspaces.length !== 1 && multiWorkspaceUI
          ? `/${RoutePaths.Workspaces}`
          : `/${RoutePaths.Workspaces}/${workspaces[0].workspaceId}`
      }
      replace
    />
  );
};

export default DefaultView;

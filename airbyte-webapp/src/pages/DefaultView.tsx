import { Navigate } from "react-router-dom";

import { useListWorkspacesInfinite } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { useRedirectFromChatConnectorBuilder } from "core/utils/useRedirectFromChatConnectorBuilder";
import { RoutePaths } from "pages/routePaths";

export const DefaultView: React.FC = () => {
  const { data: workspacesData } = useListWorkspacesInfinite(2, "", true);
  const workspaces = workspacesData?.pages.flatMap((page) => page.data.workspaces) ?? [];
  const multiWorkspaceUI = useFeature(FeatureItem.MultiWorkspaceUI);

  const connectorNavigationUrl = useRedirectFromChatConnectorBuilder(workspaces[0]?.workspaceId);

  if (connectorNavigationUrl) {
    return <Navigate to={connectorNavigationUrl} replace />;
  }

  return (
    <Navigate
      to={
        workspaces.length !== 1 && multiWorkspaceUI
          ? `/${RoutePaths.Workspaces}`
          : `/${RoutePaths.Workspaces}/${workspaces[0]?.workspaceId}`
      }
      replace
    />
  );
};

export default DefaultView;

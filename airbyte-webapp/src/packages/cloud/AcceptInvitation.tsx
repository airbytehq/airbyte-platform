import React from "react";
import { Navigate, useSearchParams } from "react-router-dom";

import { useAcceptUserInvitation } from "core/api";
import { RoutePaths } from "pages/routePaths";

export const AcceptInvitation: React.FC = () => {
  const [searchParams] = useSearchParams();
  const inviteCode = searchParams.get("inviteCode");

  const acceptedInvitation = useAcceptUserInvitation(inviteCode);

  // if accepted invitation was to a workspace, navigate there
  return (
    <Navigate
      to={`/${RoutePaths.Workspaces}/${
        acceptedInvitation?.scopeType === "workspace" ? acceptedInvitation?.scopeId : ""
      }`}
    />
  );
};

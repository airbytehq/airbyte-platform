import { Navigate, Outlet } from "react-router-dom";

import { useLocalStorage } from "core/utils/useLocalStorage";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";

// A temporary guard until SSO is rolled out everywhere
export const SSOPageGuard = () => {
  const [showSsoLogin] = useLocalStorage("airbyte_show-sso-login", false);

  if (!showSsoLogin) {
    return <Navigate to={CloudRoutes.Login} />;
  }

  return <Outlet />;
};

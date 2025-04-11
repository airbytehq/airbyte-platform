import { PropsWithChildren } from "react";
import { useLocation } from "react-router-dom";

import { useGetInstanceConfiguration } from "core/api";
import { RoutePaths } from "pages/routePaths";

import { EmbeddedAuthService } from "./EmbeddedAuthService";
import { EnterpriseAuthService } from "./EnterpriseAuthService";
import { NoAuthService } from "./NoAuthService";
import { SimpleAuthService } from "./SimpleAuthService";
/**
 * This is the auth service for OSS, including SME deployments. It will return the appropriate auth service based on the auth mode of the Airbyte instance.
 * It will also handle the case where the embedded widget is being used in SME.
 */

export const OSSAuthService: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const { auth } = useGetInstanceConfiguration();
  const location = useLocation();

  if (location.pathname === `/${RoutePaths.EmbeddedWidget}`) {
    return <EmbeddedAuthService>{children}</EmbeddedAuthService>;
  }
  if (auth.mode === "oidc") {
    return <EnterpriseAuthService>{children}</EnterpriseAuthService>;
  }
  if (auth.mode === "simple") {
    return <SimpleAuthService>{children}</SimpleAuthService>;
  }

  return <NoAuthService>{children}</NoAuthService>;
};

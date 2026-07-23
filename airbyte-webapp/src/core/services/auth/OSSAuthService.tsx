import { PropsWithChildren } from "react";

import { useGetInstanceConfiguration } from "core/api";

import { EnterpriseAuthService } from "./EnterpriseAuthService";
import { NoAuthService } from "./NoAuthService";
import { SimpleAuthService } from "./SimpleAuthService";
/**
 * This is the auth service for OSS, including SME deployments. It will return the appropriate auth service based on the auth mode of the Airbyte instance.
 */

export const OSSAuthService: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const { auth } = useGetInstanceConfiguration();

  if (auth.mode === "oidc") {
    return <EnterpriseAuthService>{children}</EnterpriseAuthService>;
  }
  if (auth.mode === "simple") {
    return <SimpleAuthService>{children}</SimpleAuthService>;
  }

  return <NoAuthService>{children}</NoAuthService>;
};

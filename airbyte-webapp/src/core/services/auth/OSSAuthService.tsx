import { PropsWithChildren } from "react";

import { useGetInstanceConfiguration } from "core/api";

import { NoAuthService } from "./NoAuthService";
import { SimpleAuthService } from "./SimpleAuthService";
/**
 * This is the auth service for OSS. It will return the appropriate auth service based on the auth mode of the Airbyte instance.
 */

export const OSSAuthService: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const { auth } = useGetInstanceConfiguration();

  if (auth.mode === "simple") {
    return <SimpleAuthService>{children}</SimpleAuthService>;
  }

  return <NoAuthService>{children}</NoAuthService>;
};

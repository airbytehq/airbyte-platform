import { PropsWithChildren } from "react";

import { FeatureItem, useFeature } from "core/services/features";

import { DefaultAuthService } from "./DefaultAuthService";
import { KeycloakAuthService } from "./KeycloakAuthService";

// This wrapper is conditionally present if the KeycloakAuthentication feature is enabled
export const OSSAuthService: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const isKeycloakAuthenticationEnabled = useFeature(FeatureItem.KeycloakAuthentication);

  return isKeycloakAuthenticationEnabled ? (
    <KeycloakAuthService>
      <DefaultAuthService>{children}</DefaultAuthService>
    </KeycloakAuthService>
  ) : (
    <DefaultAuthService>{children}</DefaultAuthService>
  );
};

import { PropsWithChildren } from "react";

import { FeatureItem, useFeature } from "core/services/features";

import { CommunityAuthService } from "./CommunityAuthService";
import { EnterpriseAuthService } from "./EnterpriseAuthService";

export const OSSAuthService: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const isKeycloakAuthenticationEnabled = useFeature(FeatureItem.KeycloakAuthentication);

  return isKeycloakAuthenticationEnabled ? (
    <EnterpriseAuthService>{children}</EnterpriseAuthService>
  ) : (
    <CommunityAuthService>{children}</CommunityAuthService>
  );
};

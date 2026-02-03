import React from "react";

import { useSetEntitlements } from "core/api";

/**
 * EntitlementsLoader component that triggers entitlement fetching and applying them via setEntitlementOverwrites.
 */
export const EntitlementsLoader: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  useSetEntitlements();

  return <>{children}</>;
};

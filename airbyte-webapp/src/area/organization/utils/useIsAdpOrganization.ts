import { useOrganization } from "core/api";

import { useCurrentOrganizationId } from "./useCurrentOrganizationId";

export const useIsAdpOrganization = (): boolean => {
  const organizationId = useCurrentOrganizationId();
  const organization = useOrganization(organizationId);
  return organization?.isAgentic ?? false;
};

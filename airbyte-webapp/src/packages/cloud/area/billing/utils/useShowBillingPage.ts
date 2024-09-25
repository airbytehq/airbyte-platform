import { useCurrentOrganizationInfo } from "core/api";

export const useShowBillingPageV2 = () => {
  const { billing } = useCurrentOrganizationInfo();
  return !!billing;
};

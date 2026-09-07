import { useIsAdpOrganization } from "area/organization/utils/useIsAdpOrganization";
import { useExperiment } from "core/services/Experiment";

export const useShowAgentsOptIn = (): boolean => {
  const optInEnabled = useExperiment("agents.externalOrgOptIn");
  const isAdpOrganization = useIsAdpOrganization();
  return optInEnabled || isAdpOrganization;
};

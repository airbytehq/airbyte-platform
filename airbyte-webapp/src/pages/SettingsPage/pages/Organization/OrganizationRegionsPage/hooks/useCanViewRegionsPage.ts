import { useCurrentOrganizationId } from "area/organization/utils";
import { useExperiment } from "core/services/Experiment";
import { FeatureItem, useFeature } from "core/services/features";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

// Gates both the navigation link and the route. The health endpoint behind the Regions page admits only the
// ManageDataplanes roles, so the page is hidden from organization readers even with the flag and feature on.
export const useCanViewRegionsPage = (): boolean => {
  const organizationId = useCurrentOrganizationId();
  const isDataplaneHealthUiEnabled = useExperiment("platform.dataplane-health-ui");
  const supportsSelfManagedRegions = useFeature(FeatureItem.SelfManagedRegions);
  const canManageDataplanes = useGeneratedIntent(Intent.ManageDataplanes, { organizationId });

  return isDataplaneHealthUiEnabled && supportsSelfManagedRegions && canManageDataplanes;
};

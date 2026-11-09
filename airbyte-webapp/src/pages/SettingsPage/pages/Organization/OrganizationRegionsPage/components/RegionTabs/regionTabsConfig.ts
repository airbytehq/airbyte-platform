import { IconType } from "components/ui/Icon";

export const REGION_TABS = ["dataplanes", "workspaces"] as const;

export type RegionTab = (typeof REGION_TABS)[number];

export const TAB_LABEL: Record<RegionTab, { icon: IconType; messageId: string; messageIdWithoutCount: string }> = {
  dataplanes: {
    icon: "database",
    messageId: "nestedRegions.dataPlanesTab",
    messageIdWithoutCount: "settings.organization.regions.tab.dataplanes",
  },
  workspaces: {
    icon: "suitcase",
    messageId: "nestedRegions.workspacesTab",
    messageIdWithoutCount: "settings.organizationSettings.regions.workspaces",
  },
};

import React from "react";
import { FormattedMessage } from "react-intl";

import { TAB_LABEL, RegionTab } from "./regionTabsConfig";

interface TabLabelProps {
  tab: RegionTab;
  count?: number;
}

export const TabLabel: React.FC<TabLabelProps> = ({ tab, count }) => {
  if (count === undefined) {
    return <FormattedMessage id={TAB_LABEL[tab].messageIdWithoutCount} />;
  }
  return <FormattedMessage id={TAB_LABEL[tab].messageId} values={{ count }} />;
};

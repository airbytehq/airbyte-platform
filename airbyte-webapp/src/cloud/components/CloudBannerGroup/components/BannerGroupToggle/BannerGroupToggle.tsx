import { FormattedMessage } from "react-intl";

import { Icon } from "components/ui/Icon";

import styles from "./BannerGroupToggle.module.scss";

interface BannerGroupToggleProps {
  isCollapsed: boolean;
  hiddenCount: number;
  onToggle: () => void;
}

export const BannerGroupToggle: React.FC<BannerGroupToggleProps> = ({ isCollapsed, hiddenCount, onToggle }) => (
  <button
    type="button"
    aria-expanded={!isCollapsed}
    className={styles.toggleRow}
    data-testid={isCollapsed ? "cloud-banner-group-collapsed" : "cloud-banner-group-expanded"}
    onClick={onToggle}
  >
    {isCollapsed ? (
      <FormattedMessage id="cloud.banners.showMore" values={{ count: hiddenCount }} />
    ) : (
      <FormattedMessage id="cloud.banners.showLess" />
    )}
    <Icon type={isCollapsed ? "chevronDown" : "chevronUp"} />
  </button>
);

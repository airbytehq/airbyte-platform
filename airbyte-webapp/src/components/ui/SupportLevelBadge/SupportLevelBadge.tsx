import classNames from "classnames";
import { FormattedMessage } from "react-intl";

import { SupportLevel } from "core/request/AirbyteClient";

import styles from "./SupportLevelBadge.module.scss";
import { Text } from "../Text";
import { Tooltip } from "../Tooltip";

type IconSize = "small" | "medium";

interface SupportLevelBadgeProps {
  supportLevel?: SupportLevel;
  custom?: boolean;
  size?: IconSize;
  tooltip?: boolean;
}

export const SupportLevelBadge: React.FC<SupportLevelBadgeProps> = ({
  supportLevel,
  custom = false,
  size = "medium",
  tooltip = true,
}) => {
  if (!supportLevel || (!custom && supportLevel === SupportLevel.none)) {
    return null;
  }

  const badge = (
    <Text
      className={classNames(styles.badge, {
        [styles["badge--certified"]]: !custom && supportLevel === "certified",
        [styles["badge--community"]]: !custom && supportLevel === "community",
        [styles["badge--custom"]]: custom,
        [styles["badge--small"]]: size === "small",
        [styles["badge--medium"]]: size === "medium",
      })}
      bold
    >
      <FormattedMessage
        id={
          custom
            ? "connector.supportLevel.custom"
            : supportLevel === "certified"
            ? "connector.supportLevel.certified"
            : "connector.supportLevel.community"
        }
      />
    </Text>
  );

  return tooltip ? (
    <Tooltip
      control={badge}
      containerClassName={classNames({
        [styles.smallTooltip]: size === "small",
        [styles.mediumTooltip]: size === "medium",
      })}
      placement="top"
    >
      <FormattedMessage
        id={
          custom
            ? "connector.supportLevel.custom.description"
            : supportLevel === "certified"
            ? "connector.supportLevel.certified.description"
            : "connector.supportLevel.community.description"
        }
      />
    </Tooltip>
  ) : (
    badge
  );
};

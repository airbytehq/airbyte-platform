import classNames from "classnames";
import { FormattedMessage } from "react-intl";

import { ReleaseStageBadge } from "components/ReleaseStageBadge";

import { useIsFCPEnabled } from "core/api/cloud";
import { ReleaseStage, SupportLevel } from "core/request/AirbyteClient";

import styles from "./SupportLevelBadge.module.scss";
import { Text } from "../Text";
import { Tooltip } from "../Tooltip";

type IconSize = "small" | "medium";

interface SupportLevelBadgeProps {
  supportLevel?: SupportLevel;
  custom?: boolean;
  size?: IconSize;
  tooltip?: boolean;

  // TODO: remove when sweeping the FCP feature flag
  releaseStage?: ReleaseStage;
}

export const SupportLevelBadge: React.FC<SupportLevelBadgeProps> = ({
  supportLevel,
  custom = false,
  size = "medium",
  tooltip = true,
  releaseStage,
}) => {
  const isFCPEnabled = useIsFCPEnabled();
  if (isFCPEnabled) {
    return <ReleaseStageBadge stage={releaseStage} small={size === "small"} tooltip={tooltip} />;
  }

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

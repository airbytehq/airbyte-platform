import classNames from "classnames";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Text } from "components/ui/Text";

import styles from "./ContextLayerSettingLabel.module.scss";

export type ContextLayerSetting = "agentAccess" | "semanticSearch";

interface ContextLayerSettingLabelProps {
  setting: ContextLayerSetting;
  actorType: "source" | "destination";
  variant?: "setup";
}

export const ContextLayerSettingLabel: React.FC<ContextLayerSettingLabelProps> = ({ setting, actorType, variant }) => (
  <div className={classNames(styles.text, { [styles.setup]: variant === "setup" })}>
    <Text className={styles.title} size={variant === "setup" ? "lg" : "sm"} bold={variant !== "setup"}>
      <FormattedMessage id={`cloud.contextLayer.${variant === "setup" ? "setup." : ""}${setting}.title`} />
    </Text>
    <Text className={styles.description} size="sm" color="grey">
      <FormattedMessage
        id={`cloud.contextLayer.${variant === "setup" ? "setup." : ""}${setting}.description`}
        values={{ actorType }}
      />
    </Text>
  </div>
);

export const useContextLayerSettingTitle = (setting: ContextLayerSetting, variant?: "setup") => {
  const { formatMessage } = useIntl();

  return formatMessage({ id: `cloud.contextLayer.${variant === "setup" ? "setup." : ""}${setting}.title` });
};

import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Text } from "components/ui/Text";

import styles from "./ContextLayerSettingLabel.module.scss";

export type ContextLayerSetting = "agentAccess" | "semanticSearch";

interface ContextLayerSettingLabelProps {
  setting: ContextLayerSetting;
  actorType: "source" | "destination";
}

export const ContextLayerSettingLabel: React.FC<ContextLayerSettingLabelProps> = ({ setting, actorType }) => (
  <div className={styles.text}>
    <Text className={styles.title} size="sm" bold>
      <FormattedMessage id={`cloud.contextLayer.${setting}.title`} />
    </Text>
    <Text className={styles.description} size="sm" color="grey">
      <FormattedMessage id={`cloud.contextLayer.${setting}.description`} values={{ actorType }} />
    </Text>
  </div>
);

export const useContextLayerSettingTitle = (setting: ContextLayerSetting) => {
  const { formatMessage } = useIntl();

  return formatMessage({ id: `cloud.contextLayer.${setting}.title` });
};

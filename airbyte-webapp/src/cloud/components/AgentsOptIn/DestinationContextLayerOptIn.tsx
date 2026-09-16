import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useAgentsProvisioningStatus, useAgentsSupportedDestinationDefinitionIds } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import styles from "./SourceContextLayerOptIn.module.scss";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

interface DestinationContextLayerOptInProps {
  destinationDefinitionId?: string;
  value: boolean;
  onChange: (value: boolean) => void;
}

const DestinationContextLayerOptInContent: React.FC<DestinationContextLayerOptInProps> = ({
  destinationDefinitionId,
  value,
  onChange,
}) => {
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp && showAgentsOptIn });
  const isEnrolled = status?.is_enrolled === true;
  const supportedDestinationDefinitionIds = useAgentsSupportedDestinationDefinitionIds();
  const canManage = useGeneratedIntent(Intent.CreateOrEditDestination);
  const { formatMessage } = useIntl();

  if (!isCloudApp || !showAgentsOptIn || !destinationDefinitionId) {
    return null;
  }

  const supported = supportedDestinationDefinitionIds.has(destinationDefinitionId ?? "");
  const agentAccess = isEnrolled && supported && value;
  const withPermissionTooltip = (control: React.ReactElement) => (
    <div className={styles.control}>
      {!isEnrolled ? (
        <Tooltip placement="bottom" control={control}>
          <FormattedMessage id="cloud.contextLayer.actor.notEnrolled" />
        </Tooltip>
      ) : !supported ? (
        <Tooltip placement="bottom" control={control}>
          <FormattedMessage id="cloud.contextLayer.actor.notSupported" />
        </Tooltip>
      ) : !canManage ? (
        <Tooltip placement="bottom" control={control}>
          <FormattedMessage id="cloud.contextLayer.destinationOptIn.noPermission" />
        </Tooltip>
      ) : (
        control
      )}
    </div>
  );

  return (
    <div className={styles.card}>
      <div className={styles.row}>
        <div className={styles.text}>
          <Text className={styles.title} size="sm" bold>
            <FormattedMessage id="cloud.contextLayer.destinationOptIn.title" />
          </Text>
          <Text className={styles.description} size="sm" color="grey">
            <FormattedMessage id="cloud.contextLayer.destinationOptIn.description" />
          </Text>
        </div>
        {withPermissionTooltip(
          <Switch
            size="sm"
            checked={agentAccess}
            disabled={!isEnrolled || !supported || !canManage}
            onChange={isEnrolled && supported && canManage ? (event) => onChange(event.target.checked) : undefined}
            aria-label={formatMessage({ id: "cloud.contextLayer.destinationOptIn.title" })}
          />
        )}
      </div>
      {!supported && (
        <Text className={styles.unsupported} size="xs" color="grey">
          <FormattedMessage id="cloud.contextLayer.actor.notSupported" />
        </Text>
      )}
    </div>
  );
};

export const DestinationContextLayerOptIn: React.FC<DestinationContextLayerOptInProps> = (props) => (
  <React.Suspense>
    <DestinationContextLayerOptInContent {...props} />
  </React.Suspense>
);

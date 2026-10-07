import React from "react";
import { FormattedMessage } from "react-intl";

import { Switch } from "components/ui/Switch";
import { Tooltip } from "components/ui/Tooltip";

import { useAgentsProvisioningStatus, useAgentsSupportedDestinationDefinitionIds } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { ContextLayerSettingLabel, useContextLayerSettingTitle } from "./ContextLayerSettingLabel";
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
  const agentAccessTitle = useContextLayerSettingTitle("agentAccess", "setup");

  if (!isCloudApp || !showAgentsOptIn || !destinationDefinitionId) {
    return null;
  }

  if (!supportedDestinationDefinitionIds.has(destinationDefinitionId)) {
    return null;
  }

  const agentAccess = isEnrolled && value;
  const withPermissionTooltip = (control: React.ReactElement) => (
    <div className={styles.control}>
      {!isEnrolled ? (
        <Tooltip placement="bottom" control={control}>
          <FormattedMessage id="cloud.contextLayer.actor.notEnrolled" />
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
    <div className={styles.cards}>
      <div className={styles.card}>
        <ContextLayerSettingLabel setting="agentAccess" actorType="destination" variant="setup" />
        {withPermissionTooltip(
          <Switch
            size="sm"
            checked={agentAccess}
            disabled={!isEnrolled || !canManage}
            onChange={isEnrolled && canManage ? (event) => onChange(event.target.checked) : undefined}
            aria-label={agentAccessTitle}
          />
        )}
      </div>
    </div>
  );
};

export const DestinationContextLayerOptIn: React.FC<DestinationContextLayerOptInProps> = (props) => (
  <React.Suspense>
    <DestinationContextLayerOptInContent {...props} />
  </React.Suspense>
);

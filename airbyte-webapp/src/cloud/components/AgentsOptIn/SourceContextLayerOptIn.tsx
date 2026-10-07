import React from "react";
import { FormattedMessage } from "react-intl";

import { Switch } from "components/ui/Switch";
import { Tooltip } from "components/ui/Tooltip";

import { useAgentsProvisioningStatus, useAgentsSupportedSourceDefinitionIds } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { ContextLayerSettingLabel, useContextLayerSettingTitle } from "./ContextLayerSettingLabel";
import styles from "./SourceContextLayerOptIn.module.scss";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

interface SourceContextLayerOptInProps {
  sourceDefinitionId?: string;
  value: boolean;
  onChange: (value: boolean) => void;
}

const SourceContextLayerOptInContent: React.FC<SourceContextLayerOptInProps> = ({
  sourceDefinitionId,
  value,
  onChange,
}) => {
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp && showAgentsOptIn });
  const isEnrolled = status?.is_enrolled === true;
  const supportedSourceDefinitionIds = useAgentsSupportedSourceDefinitionIds();
  const canManage = useGeneratedIntent(Intent.CreateOrEditSource);
  const agentAccessTitle = useContextLayerSettingTitle("agentAccess", "setup");

  if (!isCloudApp || !showAgentsOptIn) {
    return null;
  }
  if (!sourceDefinitionId) {
    return null;
  }

  if (!supportedSourceDefinitionIds.has(sourceDefinitionId)) {
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
          <FormattedMessage id="cloud.contextLayer.sourceOptIn.noPermission" />
        </Tooltip>
      ) : (
        control
      )}
    </div>
  );

  return (
    <div className={styles.cards}>
      <div className={styles.card} role="group" aria-label={agentAccessTitle}>
        <ContextLayerSettingLabel setting="agentAccess" actorType="source" variant="setup" />
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

export const SourceContextLayerOptIn: React.FC<SourceContextLayerOptInProps> = (props) => (
  <React.Suspense>
    <SourceContextLayerOptInContent {...props} />
  </React.Suspense>
);

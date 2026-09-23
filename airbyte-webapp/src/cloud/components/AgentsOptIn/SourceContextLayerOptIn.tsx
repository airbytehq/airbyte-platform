import React from "react";
import { FormattedMessage } from "react-intl";

import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useAgentsProvisioningStatus, useAgentsSupportedSourceDefinitionIds } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import { ContextLayerSettingLabel, useContextLayerSettingTitle } from "./ContextLayerSettingLabel";
import styles from "./SourceContextLayerOptIn.module.scss";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

export interface SourceContextLayerOptInValue {
  agentAccess: boolean;
  semanticSearch: boolean;
}

interface SourceContextLayerOptInProps {
  sourceDefinitionId?: string;
  value: SourceContextLayerOptInValue;
  onChange: (value: SourceContextLayerOptInValue) => void;
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
  const semanticSearchTitle = useContextLayerSettingTitle("semanticSearch", "setup");

  if (!isCloudApp || !showAgentsOptIn) {
    return null;
  }
  if (!sourceDefinitionId) {
    return null;
  }

  const supported = supportedSourceDefinitionIds.has(sourceDefinitionId);
  const agentAccess = isEnrolled && supported && value.agentAccess;
  const semanticSearch = isEnrolled && supported && value.agentAccess && value.semanticSearch;
  const withPermissionTooltip = (control: React.ReactElement) => (
    <div className={styles.control}>
      {!isEnrolled ? (
        <Tooltip placement="bottom" control={control}>
          <FormattedMessage id="cloud.contextLayer.sourceOptIn.notEnrolled" />
        </Tooltip>
      ) : !supported ? (
        <Tooltip placement="bottom" control={control}>
          <FormattedMessage id="cloud.contextLayer.actor.notSupported" />
        </Tooltip>
      ) : supported && !canManage ? (
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
            disabled={!isEnrolled || !supported || !canManage}
            onChange={
              isEnrolled && supported && canManage
                ? (event) => onChange({ ...value, agentAccess: event.target.checked })
                : undefined
            }
            aria-label={agentAccessTitle}
          />
        )}
      </div>
      <div className={styles.card} role="group" aria-label={semanticSearchTitle}>
        <ContextLayerSettingLabel setting="semanticSearch" actorType="source" variant="setup" />
        {withPermissionTooltip(
          <Switch
            size="sm"
            checked={semanticSearch}
            disabled={!isEnrolled || !supported || !value.agentAccess || !canManage}
            onChange={
              isEnrolled && supported && value.agentAccess && canManage
                ? (event) => onChange({ ...value, semanticSearch: event.target.checked })
                : undefined
            }
            aria-label={semanticSearchTitle}
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

export const SourceContextLayerOptIn: React.FC<SourceContextLayerOptInProps> = (props) => (
  <React.Suspense>
    <SourceContextLayerOptInContent {...props} />
  </React.Suspense>
);

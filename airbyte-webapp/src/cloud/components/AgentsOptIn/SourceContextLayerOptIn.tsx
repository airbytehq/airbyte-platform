import classNames from "classnames";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useAgentsProvisioningStatus, useAgentsSupportedSourceDefinitions } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import styles from "./SourceContextLayerOptIn.module.scss";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

export interface SourceContextLayerOptInValue {
  agentAccess: boolean;
  semanticSearch: boolean;
}

interface SourceContextLayerOptInProps {
  sourceDefinitionName?: string;
  value: SourceContextLayerOptInValue;
  onChange: (value: SourceContextLayerOptInValue) => void;
}

const SourceContextLayerOptInContent: React.FC<SourceContextLayerOptInProps> = ({
  sourceDefinitionName,
  value,
  onChange,
}) => {
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp && showAgentsOptIn });
  const isEnrolled = status?.is_enrolled === true;
  const supportedSourceDefinitions = useAgentsSupportedSourceDefinitions();
  const canManage = useGeneratedIntent(Intent.CreateOrEditSource);
  const { formatMessage } = useIntl();

  if (!isCloudApp || !showAgentsOptIn) {
    return null;
  }
  if (!sourceDefinitionName) {
    return null;
  }

  const supported = supportedSourceDefinitions.has(sourceDefinitionName);
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
    <div className={styles.card}>
      <div className={styles.row}>
        <div className={styles.text}>
          <Text className={styles.title} size="sm" bold>
            <FormattedMessage id="cloud.contextLayer.sourceOptIn.title" />
          </Text>
          <Text className={styles.description} size="sm" color="grey">
            <FormattedMessage id="cloud.contextLayer.sourceOptIn.description" />
          </Text>
        </div>
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
            aria-label={formatMessage({ id: "cloud.contextLayer.sourceOptIn.title" })}
          />
        )}
      </div>
      <div className={classNames(styles.row, styles.tierTwo)}>
        <div className={styles.text}>
          <Text className={styles.title} size="sm" bold>
            <FormattedMessage id="cloud.contextLayer.sourceOptIn.semanticSearch.title" />
          </Text>
          <Text className={styles.description} size="sm" color="grey">
            <FormattedMessage id="cloud.contextLayer.sourceOptIn.semanticSearch.description" />
          </Text>
        </div>
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
            aria-label={formatMessage({ id: "cloud.contextLayer.sourceOptIn.semanticSearch.title" })}
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

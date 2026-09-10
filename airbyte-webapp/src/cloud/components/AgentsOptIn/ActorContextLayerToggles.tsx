import React, { useEffect, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { StatusIcon } from "components/ui/StatusIcon";
import { Switch } from "components/ui/Switch";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useExternalWorkspaceConnectors, useAgentsProvisioningStatus, useSetExternalActorEnabled } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import styles from "./ActorContextLayerToggles.module.scss";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

type ActorType = "source" | "destination";

interface ActorContextLayerToggleProps {
  actorId: string;
  actorType: ActorType;
}

export const useShowActorContextLayerToggles = (): boolean => {
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();

  return isCloudApp && showAgentsOptIn;
};

const ActorAgentAccessToggleContent: React.FC<ActorContextLayerToggleProps> = ({ actorId, actorType }) => {
  const isVisible = useShowActorContextLayerToggles();
  const workspaceId = useCurrentWorkspaceId();
  const provisioningStatus = useAgentsProvisioningStatus({ enabled: isVisible });
  const isEnrolled = provisioningStatus?.is_enrolled === true;
  const canManage = useGeneratedIntent(
    actorType === "source" ? Intent.CreateOrEditSource : Intent.CreateOrEditDestination
  );
  const { formatMessage } = useIntl();
  const { sources, destinations, isLoading, sourcesError, destinationsError } = useExternalWorkspaceConnectors(
    workspaceId,
    {
      enabled: isVisible && isEnrolled,
    }
  );
  const { mutateAsync: setExternalActorEnabled } = useSetExternalActorEnabled();
  const [optimisticEnabled, setOptimisticEnabled] = useState<boolean>();
  const [status, setStatus] = useState<"loading" | "success" | "warning">();
  const [errorMessage, setErrorMessage] = useState<string>();

  useEffect(() => {
    if (status !== "success") {
      return;
    }

    const timeout = setTimeout(() => setStatus(undefined), 3000);
    return () => clearTimeout(timeout);
  }, [status]);

  if (!isVisible) {
    return null;
  }

  const connector = (actorType === "source" ? sources : destinations).find(({ id }) => id === actorId);
  const queryError = actorType === "source" ? sourcesError : destinationsError;
  const unavailable = !isEnrolled || queryError;
  const checked =
    !unavailable && !isLoading && connector?.supported === true && (optimisticEnabled ?? connector.enabled);
  const disabled = unavailable || isLoading || !connector || !connector.supported || !canManage || status === "loading";

  const handleChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const enabled = event.target.checked;
    setStatus("loading");
    setErrorMessage(undefined);
    setOptimisticEnabled(enabled);

    try {
      await setExternalActorEnabled({
        actorId,
        actorKind: actorType,
        enabled,
      });
      setOptimisticEnabled(undefined);
      setStatus("success");
    } catch (error) {
      setOptimisticEnabled(undefined);
      setErrorMessage(error instanceof Error && error.message ? error.message : undefined);
      setStatus("warning");
    }
  };

  const switchControl = (
    <span className={styles.control}>
      <Switch
        size="sm"
        checked={checked}
        disabled={disabled}
        onChange={!unavailable && canManage ? handleChange : undefined}
        onClick={(event) => event.stopPropagation()}
        onKeyDown={(event) => event.stopPropagation()}
        aria-label={formatMessage({ id: "cloud.contextLayer.actor.agentAccess" })}
      />
    </span>
  );

  const switchOrTooltip = !isEnrolled ? (
    <Tooltip placement="bottom" control={switchControl}>
      <FormattedMessage id="cloud.contextLayer.actor.notEnrolled" />
    </Tooltip>
  ) : queryError ? (
    <Tooltip placement="bottom" control={switchControl}>
      <FormattedMessage id="cloud.contextLayer.actor.loadFailed" />
    </Tooltip>
  ) : connector && !connector.supported ? (
    <Tooltip placement="bottom" control={switchControl}>
      <FormattedMessage id="cloud.contextLayer.actor.notSupported" />
    </Tooltip>
  ) : !isLoading && connector && !canManage ? (
    <Tooltip placement="bottom" control={switchControl}>
      <FormattedMessage id="cloud.contextLayer.actor.noPermission" />
    </Tooltip>
  ) : (
    switchControl
  );
  const statusTitle =
    status === "loading"
      ? formatMessage({ id: "cloud.contextLayer.actor.status.saving" })
      : status === "success"
      ? formatMessage({ id: "cloud.contextLayer.actor.status.saved" })
      : errorMessage ?? formatMessage({ id: "cloud.contextLayer.actor.status.failed" });

  return (
    <FlexContainer alignItems="center" gap="sm" onClick={(event) => event.stopPropagation()}>
      {switchOrTooltip}
      {status && <StatusIcon size="sm" status={status} title={statusTitle} />}
    </FlexContainer>
  );
};

export const ActorAgentAccessToggle: React.FC<ActorContextLayerToggleProps> = (props) => (
  <React.Suspense>
    <ActorAgentAccessToggleContent {...props} />
  </React.Suspense>
);

const ActorSemanticSearchToggleContent: React.FC<ActorContextLayerToggleProps> = () => {
  const isVisible = useShowActorContextLayerToggles();
  const status = useAgentsProvisioningStatus({ enabled: isVisible });
  const isEnrolled = status?.is_enrolled === true;
  const { formatMessage } = useIntl();

  if (!isVisible) {
    return null;
  }

  const switchControl = (
    <span className={styles.control}>
      <Switch
        size="sm"
        checked={false}
        disabled
        onClick={(event) => event.stopPropagation()}
        onKeyDown={(event) => event.stopPropagation()}
        aria-label={formatMessage({ id: "cloud.contextLayer.actor.semanticSearch" })}
      />
    </span>
  );

  return (
    <Tooltip placement="bottom" control={switchControl}>
      {isEnrolled ? (
        <FormattedMessage id="cloud.contextLayer.actor.semanticSearch.comingSoon" />
      ) : (
        <FormattedMessage id="cloud.contextLayer.actor.notEnrolled" />
      )}
    </Tooltip>
  );
};

export const ActorSemanticSearchToggle: React.FC<ActorContextLayerToggleProps> = (props) => (
  <React.Suspense>
    <ActorSemanticSearchToggleContent {...props} />
  </React.Suspense>
);

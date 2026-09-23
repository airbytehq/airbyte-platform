import React, { useEffect, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { StatusIcon } from "components/ui/StatusIcon";
import { Switch } from "components/ui/Switch";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import {
  useFusionWorkspaceConnectors,
  useAgentsProvisioningStatus,
  useSetFusionActorEnablement,
  useFusionActorEnablement,
  useFusionActorSaving,
  fusionEnablementState,
} from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

import styles from "./ActorContextLayerToggles.module.scss";
import { useContextLayerSettingTitle } from "./ContextLayerSettingLabel";
import { useConfirmContextLayerDisable } from "./useConfirmContextLayerDisable";
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

const ActorAgentAccessToggleContent: React.FC<
  ActorContextLayerToggleProps & { field?: "enable_agent_access" | "enable_indexing" }
> = ({ actorId, actorType, field = "enable_agent_access" }) => {
  const isVisible = useShowActorContextLayerToggles();
  const workspaceId = useCurrentWorkspaceId();
  const provisioningStatus = useAgentsProvisioningStatus({ enabled: isVisible });
  const isEnrolled = provisioningStatus?.is_enrolled === true;
  const canManage = useGeneratedIntent(
    actorType === "source" ? Intent.CreateOrEditSource : Intent.CreateOrEditDestination
  );
  const { formatMessage } = useIntl();
  const { sources, destinations, isLoading, sourcesError, destinationsError } = useFusionWorkspaceConnectors(
    workspaceId,
    {
      enabled: isVisible && isEnrolled,
      hydrate: false,
    }
  );
  const actor = { actorId, actorKind: actorType, workspaceId };
  const enablement = useFusionActorEnablement(actor, isVisible && isEnrolled);
  const saving = useFusionActorSaving(actor);
  const values = enablement.data ? fusionEnablementState(enablement.data) : undefined;
  const { mutateAsync: setFusionActorEnablement } = useSetFusionActorEnablement();
  const confirmDisable = useConfirmContextLayerDisable();
  const agentAccessTitle = useContextLayerSettingTitle("agentAccess");
  const semanticSearchTitle = useContextLayerSettingTitle("semanticSearch");
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
  const queryError = enablement.isError || (actorType === "source" ? sourcesError : destinationsError);
  const unavailable = !isEnrolled || queryError;
  const checked =
    !unavailable &&
    !enablement.isLoading &&
    connector?.supported === true &&
    (optimisticEnabled ?? values?.[field] === true);
  const disabled =
    unavailable ||
    isLoading ||
    enablement.isLoading ||
    !connector ||
    !connector.supported ||
    !canManage ||
    saving ||
    status === "loading" ||
    (field !== "enable_agent_access" && !values?.enable_agent_access);

  const handleChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
    if (!values) {
      return;
    }
    const enabled = event.target.checked;
    if (field === "enable_agent_access" && !enabled && !(await confirmDisable(connector?.name ?? ""))) {
      return;
    }
    setStatus("loading");
    setErrorMessage(undefined);
    setOptimisticEnabled(enabled);

    try {
      await setFusionActorEnablement({
        actorId,
        actorKind: actorType,
        workspaceId,
        state:
          field === "enable_agent_access"
            ? {
                ...values,
                enable_agent_access: enabled,
                enable_indexing: actorType === "destination" ? enabled : enabled && values.enable_indexing,
                ...("enable_backfill" in values ? { enable_backfill: enabled && values.enable_backfill } : {}),
              }
            : { ...values, [field]: enabled },
        expectedState: values,
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
        aria-label={field === "enable_indexing" ? semanticSearchTitle : agentAccessTitle}
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

export const ActorSemanticSearchToggle: React.FC<ActorContextLayerToggleProps> = (props) => (
  <React.Suspense>
    <ActorAgentAccessToggleContent {...props} field="enable_indexing" />
  </React.Suspense>
);

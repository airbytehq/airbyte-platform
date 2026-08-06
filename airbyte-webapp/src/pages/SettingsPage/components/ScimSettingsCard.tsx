import { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon, IconType } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useScimSettingsAccess } from "area/organization/utils";
import { useEnableScim } from "core/api";
import { ScimConfigStatus, ScimIdpProvider } from "core/api/types/AirbyteClient";
import { useModalService } from "core/services/Modal";
import { useNotificationService } from "core/services/Notification";

import { CollapsibleSettingsCard } from "./CollapsibleSettingsCard";
import { ScimCredentialsModal } from "./ScimCredentialsModal";
import { ScimIdpProviderSelector } from "./ScimIdpProviderSelector";

const STATUS_BADGE_VARIANT: Record<ScimConfigStatus, "grey" | "green" | "red"> = {
  [ScimConfigStatus.not_configured]: "grey",
  [ScimConfigStatus.enabled]: "green",
  [ScimConfigStatus.disabled]: "red",
};

const STATUS_LABEL_IDS: Record<ScimConfigStatus, string> = {
  [ScimConfigStatus.not_configured]: "settings.organizationSettings.scim.status.notConfigured",
  [ScimConfigStatus.enabled]: "settings.organizationSettings.scim.status.enabled",
  [ScimConfigStatus.disabled]: "settings.organizationSettings.scim.status.disabled",
};

const STATUS_CHIP_ICONS: Partial<Record<ScimConfigStatus, IconType>> = {
  [ScimConfigStatus.enabled]: "check",
  [ScimConfigStatus.disabled]: "cross",
};

const SCIM_ENABLE_ERROR_NOTIFICATION_ID = "scim-enable-error";

/**
 * Owns the SCIM `CollapsibleSettingsCard` (chrome + data access). The page shell that mounts
 * this component (`SSOAndScimOrganizationSettingsPage`) must stay hook-free - its tests assert
 * that no SCIM data hooks run from the page itself.
 *
 * Renders nothing at all (no card shell) when the org isn't entitled, the access hook can't
 * produce a config (not an org admin, request error), or the config is still loading - this card
 * is the last element on the page, so popping in for entitled orgs causes no layout shift.
 */
export const ScimSettingsCard: React.FC = () => {
  const { formatMessage } = useIntl();
  const { canManageScim, isScimAvailable, scimConfig, isLoading } = useScimSettingsAccess();
  const { openModal } = useModalService();
  const { registerNotification } = useNotificationService();
  const enableScim = useEnableScim();
  const [selectedProvider, setSelectedProvider] = useState<ScimIdpProvider | undefined>(undefined);

  const handleEnable = async () => {
    if (!selectedProvider) {
      return;
    }

    try {
      const response = await enableScim.mutateAsync(selectedProvider);

      if (response.token) {
        const token = response.token;
        const { scimBaseUrl } = response;
        // Drive the modal's Okta-only note off the enable response's provider, not the local
        // selection state, so it always reflects what was actually persisted.
        const idpProvider = response.idpProvider ?? selectedProvider;

        await openModal<void>({
          title: formatMessage({ id: "settings.organizationSettings.scim.enable.modal.title" }),
          preventCancel: true,
          // Without this, any location change (a browser Back press) closes the modal without
          // ever resolving `openModal`, destroying the one-time token with no way to recover it.
          allowNavigation: true,
          content: ({ onComplete }) => (
            <ScimCredentialsModal
              scimBaseUrl={scimBaseUrl}
              token={token}
              idpProvider={idpProvider}
              onComplete={onComplete}
            />
          ),
        });

        // The one-time token lives in `mutation.data` until this fires. It's never read again
        // after the modal closes, so drop this component's view of it. Note this clears the
        // observer's result only - the underlying cache entry outlives the call - so it's tidiness,
        // not a guarantee the token is gone from memory.
        enableScim.reset();
      }
    } catch {
      // SCIM backend failures are plain exceptions, not HttpProblems - there's no API-provided
      // message to format, so the toast copy is entirely client-authored.
      registerNotification({
        id: SCIM_ENABLE_ERROR_NOTIFICATION_ID,
        text: formatMessage({ id: "settings.organizationSettings.scim.enable.error" }),
        type: "error",
      });
    }
  };

  if (isLoading || !scimConfig || !isScimAvailable) {
    return null;
  }

  const { status } = scimConfig;
  const canEnable = canManageScim && status === ScimConfigStatus.not_configured;
  const statusChipIcon = STATUS_CHIP_ICONS[status];

  return (
    <CollapsibleSettingsCard
      label={formatMessage({ id: "settings.organizationSettings.scim.label" })}
      status={status === ScimConfigStatus.enabled ? <Icon type="check" color="primary" /> : undefined}
    >
      <FlexContainer direction="column" gap="lg">
        <FlexContainer direction="column" gap="md">
          <Text bold size="lg">
            <FormattedMessage id="settings.organizationSettings.scim.provisioning" />
          </Text>
          <FlexContainer alignItems="center">
            <Badge variant={STATUS_BADGE_VARIANT[status]} uppercase={false}>
              {statusChipIcon ? (
                <FlexContainer gap="xs" alignItems="center">
                  <Icon type={statusChipIcon} size="xs" />
                  <FormattedMessage id={STATUS_LABEL_IDS[status]} />
                </FlexContainer>
              ) : (
                <FormattedMessage id={STATUS_LABEL_IDS[status]} />
              )}
            </Badge>
          </FlexContainer>
        </FlexContainer>

        {canEnable && (
          <FlexContainer direction="column" gap="lg">
            <Text size="sm" color="grey">
              <FormattedMessage id="settings.organizationSettings.scim.description" />
            </Text>

            <FlexContainer direction="column" gap="md">
              <ScimIdpProviderSelector value={selectedProvider} onChange={setSelectedProvider} />
              <Text size="sm" color="grey">
                <FormattedMessage id="settings.organizationSettings.scim.idpProvider.footnote" />
              </Text>
            </FlexContainer>

            <FlexContainer justifyContent="flex-end">
              <Button
                type="button"
                disabled={!selectedProvider}
                isLoading={enableScim.isLoading}
                onClick={handleEnable}
              >
                <FormattedMessage id="settings.organizationSettings.scim.enable.button" />
              </Button>
            </FlexContainer>
          </FlexContainer>
        )}
      </FlexContainer>
    </CollapsibleSettingsCard>
  );
};

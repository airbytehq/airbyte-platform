import { useState } from "react";
import { FormattedDate, FormattedMessage, useIntl } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Button } from "components/ui/Button";
import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer } from "components/ui/Flex";
import { Icon, IconType } from "components/ui/Icon";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { useScimSettingsAccess } from "area/organization/utils";
import { useDisableScim, useEnableScim, useRotateScimToken } from "core/api";
import { ScimConfigStatus, ScimIdpProvider } from "core/api/types/AirbyteClient";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { useModalService } from "core/services/Modal";
import { useNotificationService } from "core/services/Notification";
import { links } from "core/utils/links";

import { CollapsibleSettingsCard } from "./CollapsibleSettingsCard";
import { ScimCredentialsModal } from "./ScimCredentialsModal";
import { ScimIdpProviderSelector } from "./ScimIdpProviderSelector";
import styles from "./ScimSettingsCard.module.scss";

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

const SUMMARY_PROVIDER_LABEL_IDS: Record<ScimIdpProvider, string> = {
  [ScimIdpProvider.okta]: "settings.organizationSettings.scim.idpProvider.okta",
  [ScimIdpProvider.microsoft_entra_id]: "settings.organizationSettings.scim.idpProvider.microsoftEntraId",
};

const STATUS_CHIP_ICONS: Partial<Record<ScimConfigStatus, IconType>> = {
  [ScimConfigStatus.enabled]: "check",
  [ScimConfigStatus.disabled]: "cross",
};

const SCIM_ENABLE_ERROR_NOTIFICATION_ID = "scim-enable-error";
const SCIM_DISABLE_ERROR_NOTIFICATION_ID = "scim-disable-error";
const SCIM_ROTATE_ERROR_NOTIFICATION_ID = "scim-rotate-error";

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
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { registerNotification } = useNotificationService();
  const enableScim = useEnableScim();
  const disableScim = useDisableScim();
  const rotateScimToken = useRotateScimToken();
  const [selectedProvider, setSelectedProvider] = useState<ScimIdpProvider | undefined>(undefined);

  const handleEnable = async (provider: ScimIdpProvider | undefined) => {
    if (!provider) {
      return;
    }

    try {
      const response = await enableScim.mutateAsync(provider);

      if (response.token) {
        const token = response.token;
        const { scimBaseUrl } = response;
        // Drive the modal's Okta-only note off the enable response's provider, not the
        // provider this call was sent with, so it always reflects what was actually persisted.
        const resolvedProvider = response.idpProvider ?? provider;

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
              idpProvider={resolvedProvider}
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

  const handleDisable = () => {
    openConfirmationModal({
      title: "settings.organizationSettings.scim.disable.confirm.title",
      text: "settings.organizationSettings.scim.disable.confirm.text",
      submitButtonText: "settings.organizationSettings.scim.disable.confirm.button",
      submitButtonVariant: "danger",
      onSubmit: async () => {
        try {
          await disableScim.mutateAsync();
          closeConfirmationModal();
        } catch {
          // SCIM backend failures are plain exceptions, not HttpProblems - there's no API-provided
          // message to format, so the toast copy is entirely client-authored.
          registerNotification({
            id: SCIM_DISABLE_ERROR_NOTIFICATION_ID,
            text: formatMessage({ id: "settings.organizationSettings.scim.disable.error" }),
            type: "error",
          });
        }
      },
    });
  };

  const handleRotate = () => {
    openConfirmationModal({
      title: "settings.organizationSettings.scim.rotate.confirm.title",
      text: "settings.organizationSettings.scim.rotate.confirm.text",
      submitButtonText: "settings.organizationSettings.scim.rotate.confirm.button",
      submitButtonVariant: "danger",
      onSubmit: async () => {
        try {
          const response = await rotateScimToken.mutateAsync();
          closeConfirmationModal();

          if (response.token) {
            const token = response.token;
            const { scimBaseUrl: rotatedScimBaseUrl } = response;
            // Drive the modal's Okta-only note off the rotate response's provider, falling back to
            // the card's stored provider, so it always reflects what was actually persisted.
            const resolvedProvider = response.idpProvider ?? idpProvider;

            if (resolvedProvider) {
              await openModal<void>({
                title: formatMessage({ id: "settings.organizationSettings.scim.enable.modal.title" }),
                preventCancel: true,
                // Without this, any location change (a browser Back press) closes the modal without
                // ever resolving `openModal`, destroying the one-time token with no way to recover it.
                allowNavigation: true,
                content: ({ onComplete }) => (
                  <ScimCredentialsModal
                    scimBaseUrl={rotatedScimBaseUrl}
                    token={token}
                    idpProvider={resolvedProvider}
                    onComplete={onComplete}
                  />
                ),
              });

              // The one-time token lives in `mutation.data` until this fires. It's never read again
              // after the modal closes, so drop this component's view of it. Note this clears the
              // observer's result only - the underlying cache entry outlives the call - so it's tidiness,
              // not a guarantee the token is gone from memory.
              rotateScimToken.reset();
            }
          }
        } catch {
          // SCIM backend failures are plain exceptions, not HttpProblems - there's no API-provided
          // message to format, so the toast copy is entirely client-authored.
          registerNotification({
            id: SCIM_ROTATE_ERROR_NOTIFICATION_ID,
            text: formatMessage({ id: "settings.organizationSettings.scim.rotate.error" }),
            type: "error",
          });
        }
      },
    });
  };

  if (isLoading || !scimConfig || !isScimAvailable) {
    return null;
  }

  const { status, idpProvider, scimBaseUrl, createdAt } = scimConfig;
  const canEnable = canManageScim && status === ScimConfigStatus.not_configured;
  const canReenable = canManageScim && status === ScimConfigStatus.disabled;
  const statusChipIcon = STATUS_CHIP_ICONS[status];

  return (
    <CollapsibleSettingsCard
      label={formatMessage({ id: "settings.organizationSettings.scim.label" })}
      docsLink={links.scimDocs}
      docsLinkLabel={formatMessage({ id: "settings.organizationSettings.scim.docsLink" })}
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
                <FlexContainer className={styles.statusChipContent} gap="xs" alignItems="center">
                  <Icon type={statusChipIcon} size="xs" />
                  <FormattedMessage id={STATUS_LABEL_IDS[status]} />
                </FlexContainer>
              ) : (
                <span className={styles.statusChipContent}>
                  <FormattedMessage id={STATUS_LABEL_IDS[status]} />
                </span>
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
                onClick={() => handleEnable(selectedProvider)}
              >
                <FormattedMessage id="settings.organizationSettings.scim.enable.button" />
              </Button>
            </FlexContainer>
          </FlexContainer>
        )}

        {canReenable && (
          <FlexContainer direction="column" gap="lg">
            <Text size="sm" color="grey">
              <FormattedMessage id="settings.organizationSettings.scim.description" />
            </Text>

            <FlexContainer direction="column" gap="md">
              <Text size="sm" bold>
                <FormattedMessage id="settings.organizationSettings.scim.idpProvider.label" />
              </Text>
              <ScimIdpProviderSelector value={idpProvider} onChange={() => {}} disabled />
            </FlexContainer>

            <Message
              type="warning"
              text={formatMessage({ id: "settings.organizationSettings.scim.disabled.changeProviderNote" })}
            />

            <FlexContainer justifyContent="flex-end">
              <Button
                type="button"
                disabled={!idpProvider}
                isLoading={enableScim.isLoading}
                onClick={() => handleEnable(idpProvider)}
              >
                <FormattedMessage id="settings.organizationSettings.scim.enable.button" />
              </Button>
            </FlexContainer>
          </FlexContainer>
        )}

        {status === ScimConfigStatus.enabled && (
          <FlexContainer direction="column" gap="lg">
            <div className={styles.summaryTable}>
              <div className={styles.summaryRow}>
                <Text color="grey">
                  <FormattedMessage id="settings.organizationSettings.scim.summary.idpProvider" />
                </Text>
                <Text>
                  <FormattedMessage
                    id={
                      (idpProvider && SUMMARY_PROVIDER_LABEL_IDS[idpProvider]) ??
                      "settings.organizationSettings.scim.summary.unknown"
                    }
                  />
                </Text>
              </div>

              <div className={styles.summaryRow}>
                <Text color="grey">
                  <FormattedMessage id="settings.organizationSettings.scim.summary.scimBaseUrl" />
                </Text>
                <FlexContainer alignItems="center" gap="lg">
                  <Text>{scimBaseUrl}</Text>
                  <CopyButton content={scimBaseUrl} />
                </FlexContainer>
              </div>

              <div className={styles.summaryRow}>
                <Text color="grey">
                  <FormattedMessage id="settings.organizationSettings.scim.summary.token" />
                </Text>
                <Text>
                  <FormattedMessage id="settings.organizationSettings.scim.summary.token.hidden" />
                </Text>
              </div>

              <div className={styles.summaryRow}>
                <Text color="grey">
                  <FormattedMessage id="settings.organizationSettings.scim.summary.enabledOn" />
                </Text>
                <Text>
                  {createdAt ? (
                    <FormattedDate value={createdAt * 1000} dateStyle="medium" />
                  ) : (
                    <FormattedMessage id="settings.organizationSettings.scim.summary.unknown" />
                  )}
                </Text>
              </div>
            </div>

            <Message type="info" text={formatMessage({ id: "settings.organizationSettings.scim.summary.info" })} />

            <FlexContainer>
              <Button type="button" variant="secondary" disabled={!idpProvider} onClick={handleRotate}>
                <FormattedMessage id="settings.organizationSettings.scim.rotate.button" />
              </Button>
              <Button type="button" variant="danger" onClick={handleDisable}>
                <FormattedMessage id="settings.organizationSettings.scim.disable.button" />
              </Button>
            </FlexContainer>
          </FlexContainer>
        )}
      </FlexContainer>
    </CollapsibleSettingsCard>
  );
};

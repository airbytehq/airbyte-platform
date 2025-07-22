import { Disclosure, DisclosureButton, DisclosurePanel } from "@headlessui/react";
import { motion, AnimatePresence } from "framer-motion";
import { useCallback } from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControl } from "components/forms/FormControl";
import { FormSubmissionButtons } from "components/forms/FormSubmissionButtons";
import { Box } from "components/ui/Box";
import { BrandingBadge } from "components/ui/BrandingBadge";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { useSSOConfigManagement } from "core/api";
import { FeatureItem, IfFeatureEnabled } from "core/services/features";
import { links } from "core/utils/links";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./SSOSettings.module.scss";
import { SSOFormValues } from "../UpdateSSOSettingsForm";

export const SSOSettings = () => {
  const { formatMessage } = useIntl();
  const { isSSOConfigured, isLoading, deleteSsoConfig } = useSSOConfigManagement();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { registerNotification } = useNotificationService();
  const { isSubmitting } = useFormState();

  const openSsoConfigRemovalModal = useCallback(() => {
    openConfirmationModal({
      text: "settings.organizationSettings.sso.delete.modal.text",
      title: "settings.organizationSettings.sso.delete.modal.title",
      submitButtonText: "settings.organizationSettings.sso.delete.confirm",
      submitButtonVariant: "danger",
      onSubmit: async () => {
        try {
          await deleteSsoConfig();
          closeConfirmationModal();
          registerNotification({
            id: "sso/delete-config-success",
            text: formatMessage({ id: "settings.organizationSettings.sso.delete.success" }),
            type: "success",
          });
        } catch (e) {
          registerNotification({
            id: "sso/delete-config-failure",
            text: formatMessage({ id: "settings.organizationSettings.sso.delete.failure" }),
            type: "error",
          });
        }
      },
    });
  }, [openConfirmationModal, deleteSsoConfig, closeConfirmationModal, registerNotification, formatMessage]);

  return (
    <div className={styles.card}>
      <Disclosure>
        {({ open }) => (
          <>
            <FlexContainer gap="md" alignItems="center">
              <DisclosureButton as="div">
                <FlexContainer gap="md" alignItems="center" className={styles.card__header}>
                  <motion.div animate={{ rotate: open ? 90 : 0 }} transition={{ duration: 0.2, ease: "easeInOut" }}>
                    <Icon type="chevronRight" />
                  </motion.div>
                  <Text bold>{formatMessage({ id: "settings.organizationSettings.sso.label" })}</Text>
                  {isLoading || isSubmitting ? (
                    <Icon type="loading" />
                  ) : isSSOConfigured ? (
                    <Icon type="check" color="primary" />
                  ) : (
                    <Text size="sm" color="grey300" italicized>
                      {formatMessage({ id: "settings.organizationSettings.sso.label.optional" })}
                    </Text>
                  )}
                  <IfFeatureEnabled feature={FeatureItem.CloudForTeamsBranding}>
                    <BrandingBadge product="cloudForTeams" testId="sso-label-cloud-for-teams-badge" />
                  </IfFeatureEnabled>
                </FlexContainer>
              </DisclosureButton>
              <ExternalLink href={links.ssoDocs} className={styles["card__header-link"]}>
                <Button
                  type="button"
                  variant="link"
                  icon="docs"
                  size="sm"
                  iconPosition="left"
                  iconSize="sm"
                  className={styles["card__header-button"]}
                >
                  <FormattedMessage id="settings.organizationSettings.sso.docsLink" />
                </Button>
              </ExternalLink>
            </FlexContainer>

            <AnimatePresence initial={false}>
              {open && (
                <motion.div
                  initial={{ height: 0, opacity: 0 }}
                  animate={{ height: "auto", opacity: 1 }}
                  exit={{ height: 0, opacity: 0 }}
                  transition={{ duration: 0.2, ease: "easeInOut" }}
                >
                  <DisclosurePanel static>
                    <Box mt="lg">
                      {isSSOConfigured ? (
                        <FlexContainer direction="column" alignItems="flex-start">
                          <Message text={formatMessage({ id: "settings.organizationSettings.sso.configured" })} />
                          <Button variant="danger" type="button" onClick={openSsoConfigRemovalModal}>
                            <FormattedMessage id="settings.organizationSettings.sso.configured.remove" />
                          </Button>
                        </FlexContainer>
                      ) : (
                        <>
                          <FormControl<SSOFormValues>
                            label={formatMessage({ id: "settings.organizationSettings.sso.companyIdentifier" })}
                            fieldType="input"
                            name="companyIdentifier"
                            required
                          />
                          <FormControl<SSOFormValues>
                            label={formatMessage({ id: "settings.organizationSettings.sso.clientId" })}
                            fieldType="input"
                            name="clientId"
                            required
                          />
                          <FormControl<SSOFormValues>
                            label={formatMessage({ id: "settings.organizationSettings.sso.clientSecret" })}
                            fieldType="input"
                            type="password"
                            name="clientSecret"
                            required
                          />
                          <FormControl<SSOFormValues>
                            label={formatMessage({ id: "settings.organizationSettings.sso.discoveryUrl" })}
                            fieldType="input"
                            name="discoveryUrl"
                            required
                          />
                          <FormControl<SSOFormValues>
                            label={formatMessage({ id: "settings.organizationSettings.sso.emailDomain" })}
                            fieldType="input"
                            name="emailDomain"
                            required
                          />

                          <FormSubmissionButtons noCancel justify="flex-start" submitKey="form.saveChanges" />
                        </>
                      )}
                    </Box>
                  </DisclosurePanel>
                </motion.div>
              )}
            </AnimatePresence>
          </>
        )}
      </Disclosure>
    </div>
  );
};

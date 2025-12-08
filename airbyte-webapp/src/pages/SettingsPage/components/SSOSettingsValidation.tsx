import type { ReactNode } from "react";

import { Disclosure, DisclosureButton, DisclosurePanel } from "@headlessui/react";
import classNames from "classnames";
import { motion, AnimatePresence } from "framer-motion";
import { useEffect, useMemo, useState } from "react";
import { useFormContext, useFormState, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControl } from "components/forms/FormControl";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Input } from "components/ui/Input";
import { ExternalLink } from "components/ui/Link";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useActivateSsoConfig, useListDomainVerifications, useSSOConfigManagement } from "core/api";
import { useFormatError } from "core/errors";
import { useAuthService } from "core/services/auth";
import { links } from "core/utils/links";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useExperiment } from "hooks/services/Experiment";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./SSOSettings.module.scss";
import { isSsoTestCallback } from "./ssoTestUtils";
import { useSSOTestCallback } from "./useSSOTestCallback";
import { SSOFormValuesValidation } from "../UpdateSSOSettingsForm";

export const SSOSettingsValidation = () => {
  const { formatMessage } = useIntl();
  const formatError = useFormatError();
  const organizationId = useCurrentOrganizationId();
  const { ssoConfig, isLoading } = useSSOConfigManagement();
  const { mutateAsync: activateSsoConfigMutation } = useActivateSsoConfig();
  const { registerNotification } = useNotificationService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const authService = useAuthService();
  const { isSubmitting } = useFormState();
  const { testResult, setTestResult } = useSSOTestCallback();
  const { setValue } = useFormContext<SSOFormValuesValidation>();
  const [emailDomainInput, setEmailDomainInput] = useState("");
  const [isActivating, setIsActivating] = useState(false);
  const [activationResult, setActivationResult] = useState<{ success: boolean; message: ReactNode } | null>(null);
  const [, setLastSsoCompanyIdentifier] = useLocalStorage("airbyte_last-sso-company-identifier", "");
  const [, setRedirectToSsoAfterLogout] = useLocalStorage("airbyte_redirect-to-sso-after-logout", "");
  const useVerifiedDomainsForActivate = useExperiment("platform.use-verified-domains-for-sso-activate");
  const { data: domainVerifications } = useListDomainVerifications();

  // Watch the companyIdentifier field to dynamically generate URIs
  const companyIdentifier = useWatch<SSOFormValuesValidation>({ name: "companyIdentifier" });

  // Memoize the constructed URIs to avoid duplicate string interpolations
  const { signInRedirectUri, signOutRedirectUri } = useMemo(() => {
    if (!companyIdentifier) {
      return { signInRedirectUri: null, signOutRedirectUri: null };
    }
    const baseUri = `https://cloud.airbyte.com/auth/realms/${companyIdentifier}/broker/default/endpoint`;
    return {
      signInRedirectUri: baseUri,
      signOutRedirectUri: `${baseUri}/logout_response`,
    };
  }, [companyIdentifier]);

  // Step 1 is complete when we have a draft config that was successfully tested,
  // or when we have a draft config with valid credentials (test was done in a previous session)
  const isStep1Complete =
    (testResult?.success === true && ssoConfig?.status === "draft") ||
    (ssoConfig?.status === "draft" && !!ssoConfig?.clientId && !!ssoConfig?.clientSecret);

  // Check if SSO is fully configured and active
  const isActive = ssoConfig?.status === "active";

  // Keep disclosure open when returning from SSO test
  const shouldBeOpen = isSsoTestCallback();

  // Memoize the email domains string to avoid unnecessary re-renders
  const emailDomainsString = useMemo(
    () => (ssoConfig?.emailDomains && ssoConfig.emailDomains.length > 0 ? ssoConfig.emailDomains.join(", ") : ""),
    [ssoConfig?.emailDomains]
  );

  // Get verified domains for the new feature flag flow
  const verifiedDomains = useMemo(
    () =>
      domainVerifications?.domainVerifications?.filter((dv) => dv.status === "VERIFIED").map((dv) => dv.domain) || [],
    [domainVerifications]
  );

  // Populate email domain field when config is active
  useEffect(() => {
    if (isActive && emailDomainsString) {
      setEmailDomainInput(emailDomainsString);
    }
  }, [isActive, emailDomainsString]);

  const handleRetest = () => {
    // Clear test result
    setTestResult(null);
    // Clear client secret and discovery URL fields
    setValue("clientSecret", "");
    setValue("discoveryUrl", "");
  };

  const getTestButtonText = () => {
    if (isSubmitting) {
      return formatMessage({ id: "settings.organizationSettings.sso.test.button.testing" });
    }
    if (isStep1Complete) {
      return formatMessage({ id: "settings.organizationSettings.sso.test.button.retest" });
    }
    return formatMessage({ id: "settings.organizationSettings.sso.test.button" });
  };

  const handleActivate = () => {
    if (!useVerifiedDomainsForActivate && !emailDomainInput) {
      return;
    }

    if (useVerifiedDomainsForActivate && verifiedDomains.length === 0) {
      registerNotification({
        id: "sso-activation-no-verified-domains",
        text: formatMessage({ id: "settings.organizationSettings.sso.activate.error.noVerifiedDomains" }),
        type: "error",
      });
      return;
    }

    const modalText = useVerifiedDomainsForActivate
      ? formatMessage(
          { id: "settings.organizationSettings.sso.activate.confirm.text.verifiedDomains" },
          { domains: verifiedDomains.join(", ") }
        )
      : formatMessage({ id: "settings.organizationSettings.sso.activate.confirm.text" });

    openConfirmationModal({
      title: formatMessage({ id: "settings.organizationSettings.sso.activate.confirm.title" }),
      text: modalText,
      submitButtonText: formatMessage({ id: "settings.organizationSettings.sso.activate.confirm.button" }),
      submitButtonVariant: "primary",
      onSubmit: async () => {
        setIsActivating(true);
        try {
          await activateSsoConfigMutation({
            organizationId,
            emailDomain: useVerifiedDomainsForActivate ? "" : emailDomainInput,
          });
          const successMessage = formatMessage({ id: "settings.organizationSettings.sso.activate.success" });

          registerNotification({
            id: "sso-activation-success",
            text: successMessage,
            type: "success",
          });

          // Wait 2 seconds to let user see the success message, then logout and redirect to SSO sign-in
          await new Promise<void>((resolve) =>
            setTimeout(() => {
              if (ssoConfig?.companyIdentifier) {
                setLastSsoCompanyIdentifier(ssoConfig.companyIdentifier);
                setRedirectToSsoAfterLogout("true");
              }
              // Logout (clears session) - this will redirect to root, but we'll intercept it
              authService.logout?.();
              resolve();
            }, 2000)
          );
        } catch (error) {
          closeConfirmationModal();
          setActivationResult({
            success: false,
            message: formatError(error),
          });
        } finally {
          setIsActivating(false);
        }
      },
    });
  };

  return (
    <div className={styles.card}>
      <Disclosure defaultOpen={shouldBeOpen}>
        {({ open }) => (
          <>
            <FlexContainer gap="md" alignItems="center">
              <DisclosureButton as="div">
                <FlexContainer gap="md" alignItems="center" className={styles.card__header}>
                  <motion.div animate={{ rotate: open ? 90 : 0 }} transition={{ duration: 0.2, ease: "easeInOut" }}>
                    <Icon type="chevronRight" />
                  </motion.div>
                  <Text bold>{formatMessage({ id: "settings.organizationSettings.sso.label" })}</Text>
                  {isLoading ? (
                    <Icon type="loading" />
                  ) : isActive ? (
                    <Icon type="check" color="primary" />
                  ) : (
                    <Text size="sm" color="grey300" italicized>
                      {formatMessage({ id: "settings.organizationSettings.sso.label.optional" })}
                    </Text>
                  )}
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
                      {isActive && (
                        <Box mb="lg">
                          <Message text={formatMessage({ id: "settings.organizationSettings.sso.configured" })} />
                        </Box>
                      )}

                      <FlexContainer direction="column" gap="xl">
                        {/* Step 1: Test your configuration */}
                        <Box
                          className={classNames(styles.step, {
                            [styles["step--active"]]: !isStep1Complete && !isActive,
                          })}
                        >
                          <FlexContainer direction="column" gap="lg">
                            <div
                              className={classNames({
                                [styles["stepContent--disabled"]]: isStep1Complete,
                              })}
                            >
                              <FlexContainer direction="column" gap="lg">
                                <Text bold size="lg">
                                  {formatMessage({ id: "settings.organizationSettings.sso.step1.title" })}
                                </Text>
                                <div>
                                  <FormControl<SSOFormValuesValidation>
                                    label={formatMessage({ id: "settings.organizationSettings.sso.companyIdentifier" })}
                                    fieldType="input"
                                    name="companyIdentifier"
                                    required
                                    disabled={isActive}
                                    reserveSpaceForError={false}
                                  />
                                  <Box mt="sm">
                                    <Message
                                      type="info"
                                      text={formatMessage({ id: "settings.organizationSettings.sso.redirectUri.info" })}
                                    >
                                      <Box p="md">
                                        <FlexContainer direction="column" gap="md">
                                          <FlexContainer direction="column" gap="xs">
                                            <FlexContainer justifyContent="space-between" alignItems="center">
                                              <Text size="sm" bold>
                                                <FormattedMessage id="settings.organizationSettings.sso.redirectUri.signIn" />
                                              </Text>
                                              {signInRedirectUri && (
                                                <CopyButton variant="clear" content={signInRedirectUri} />
                                              )}
                                            </FlexContainer>
                                            {signInRedirectUri ? (
                                              <Text size="sm" as="span" className={styles.uriText}>
                                                {signInRedirectUri}
                                              </Text>
                                            ) : (
                                              <Text size="sm" as="span" className={styles.uriText}>
                                                https://cloud.airbyte.com/auth/realms/
                                                <Text as="span" italicized color="grey400">
                                                  <FormattedMessage id="settings.organizationSettings.sso.redirectUri.placeholder" />
                                                </Text>
                                                /broker/default/endpoint
                                              </Text>
                                            )}
                                          </FlexContainer>
                                          <FlexContainer direction="column" gap="xs">
                                            <FlexContainer justifyContent="space-between" alignItems="center">
                                              <Text size="sm" bold>
                                                <FormattedMessage id="settings.organizationSettings.sso.redirectUri.signOut" />
                                              </Text>
                                              {signOutRedirectUri && (
                                                <CopyButton variant="clear" content={signOutRedirectUri} />
                                              )}
                                            </FlexContainer>
                                            {signOutRedirectUri ? (
                                              <Text size="sm" as="span" className={styles.uriText}>
                                                {signOutRedirectUri}
                                              </Text>
                                            ) : (
                                              <Text size="sm" as="span" className={styles.uriText}>
                                                https://cloud.airbyte.com/auth/realms/
                                                <Text as="span" italicized color="grey400">
                                                  <FormattedMessage id="settings.organizationSettings.sso.redirectUri.placeholder" />
                                                </Text>
                                                /broker/default/endpoint/logout_response
                                              </Text>
                                            )}
                                          </FlexContainer>
                                        </FlexContainer>
                                      </Box>
                                    </Message>
                                  </Box>
                                </div>
                                <FormControl<SSOFormValuesValidation>
                                  label={formatMessage({ id: "settings.organizationSettings.sso.clientId" })}
                                  fieldType="input"
                                  name="clientId"
                                  required
                                  disabled={isActive}
                                />
                                <FormControl<SSOFormValuesValidation>
                                  label={formatMessage({ id: "settings.organizationSettings.sso.clientSecret" })}
                                  fieldType="input"
                                  type="password"
                                  name="clientSecret"
                                  required
                                  disabled={isStep1Complete || isActive}
                                  placeholder={
                                    isStep1Complete || isActive
                                      ? formatMessage({ id: "settings.organizationSettings.sso.test.placeholder" })
                                      : undefined
                                  }
                                />
                                <FormControl<SSOFormValuesValidation>
                                  label={formatMessage({ id: "settings.organizationSettings.sso.discoveryUrl" })}
                                  fieldType="input"
                                  name="discoveryUrl"
                                  required
                                  disabled={isStep1Complete || isActive}
                                  placeholder={
                                    isStep1Complete || isActive
                                      ? formatMessage({ id: "settings.organizationSettings.sso.test.placeholder" })
                                      : undefined
                                  }
                                />
                              </FlexContainer>
                            </div>

                            <FlexContainer gap="md" alignItems="center">
                              <Button
                                type={isStep1Complete ? "button" : "submit"}
                                variant={isStep1Complete ? "secondary" : "primary"}
                                isLoading={isSubmitting}
                                onClick={isStep1Complete ? handleRetest : undefined}
                                disabled={isActive}
                              >
                                {getTestButtonText()}
                              </Button>
                              {testResult && (
                                <FlexContainer alignItems="center" gap="sm">
                                  <Icon
                                    type={testResult.success ? "statusSuccess" : "statusError"}
                                    color={testResult.success ? "success" : "error"}
                                    size="sm"
                                  />
                                  <Text>{testResult.message}</Text>
                                </FlexContainer>
                              )}
                            </FlexContainer>
                          </FlexContainer>
                        </Box>

                        {/* Step 2: Activate your configuration */}
                        <Box
                          className={classNames(styles.step, {
                            [styles["step--inactive"]]: !isStep1Complete && !isActive,
                            [styles["step--active"]]: isStep1Complete && !isActive,
                          })}
                        >
                          <FlexContainer direction="column" gap="lg">
                            <Text bold size="lg" color={!isStep1Complete && !isActive ? "grey300" : undefined}>
                              {formatMessage({ id: "settings.organizationSettings.sso.step2.title" })}
                            </Text>

                            <FlexContainer direction="column" gap="md">
                              {/* Note: Step 2 uses manual Input instead of FormControl because activation is a
                    separate action from form submission - it calls a different API endpoint after
                    the draft config has already been created and tested */}
                              {!useVerifiedDomainsForActivate && (
                                <FlexContainer direction="column" gap="xs">
                                  <Text color={!isStep1Complete && !isActive ? "grey300" : undefined}>
                                    {formatMessage({ id: "settings.organizationSettings.sso.emailDomain" })}
                                  </Text>
                                  <Input
                                    value={emailDomainInput}
                                    onChange={(e) => setEmailDomainInput(e.currentTarget.value)}
                                    disabled={!isStep1Complete || isActive}
                                    placeholder={formatMessage({
                                      id: "settings.organizationSettings.sso.emailDomain.placeholder",
                                    })}
                                  />
                                </FlexContainer>
                              )}

                              <FlexContainer alignItems="flex-start">
                                <Button
                                  type="button"
                                  variant="primary"
                                  onClick={handleActivate}
                                  isLoading={isActivating}
                                  disabled={
                                    !isStep1Complete ||
                                    (!useVerifiedDomainsForActivate && !emailDomainInput) ||
                                    isActive
                                  }
                                >
                                  {formatMessage({ id: "settings.organizationSettings.sso.activate.button" })}
                                </Button>
                                {activationResult && (
                                  <FlexContainer alignItems="center" gap="sm">
                                    <Icon
                                      type={activationResult.success ? "check" : "cross"}
                                      color={activationResult.success ? "success" : "error"}
                                    />
                                    <Text color={activationResult.success ? "green" : "red"}>
                                      {activationResult.message}
                                    </Text>
                                  </FlexContainer>
                                )}
                              </FlexContainer>
                            </FlexContainer>
                          </FlexContainer>
                        </Box>
                      </FlexContainer>
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

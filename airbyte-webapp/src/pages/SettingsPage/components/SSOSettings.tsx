import { Disclosure, DisclosureButton, DisclosurePanel } from "@headlessui/react";
import { motion, AnimatePresence } from "framer-motion";
import { useEffect, useMemo } from "react";
import { useFormContext, useFormState, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { FormControl } from "components/forms/FormControl";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { FeatureItem, IfFeatureEnabled } from "core/services/features";
import { links } from "core/utils/links";
import { BrandingBadge } from "views/layout/SideBar/AirbyteHomeLink";

import styles from "./SSOSettings.module.scss";
import { BaseOrganizationFormValues } from "../UpdateOrganizationSettingsForm";

export const ssoValidationSchema = z.object({
  emailDomain: z.string().trim().optional(),
  clientId: z.string().trim().optional(),
  clientSecret: z.string().trim().optional(),
  discoveryEndpoint: z.string().trim().optional(),
  subdomain: z.string().trim().optional(),
});

type SsoFormValues = z.infer<typeof ssoValidationSchema>;

const SSO_FIELDS = Object.keys(ssoValidationSchema.shape) as Array<keyof SsoFormValues>;

export const ssoRefinementFunction = (data: SsoFormValues & BaseOrganizationFormValues, ctx: z.RefinementCtx) => {
  // filter sso fields from data
  const ssoFields = Object.fromEntries(
    Object.entries(data).filter(([key]) => SSO_FIELDS.includes(key as keyof SsoFormValues))
  );

  const hasAnyValue = Object.values(ssoFields).some((value) => value && value.length > 0);
  // skip validation if all SSO fields are empty
  if (!hasAnyValue) {
    return;
  }

  // validate all SSO fields are filled when at least one has a value
  const emptyFields = Object.entries(ssoFields)
    .filter(([_, value]) => !value || value.length === 0)
    .map(([key]) => key);

  emptyFields.forEach((field) => {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: "form.empty.error",
      path: [field],
    });
  });
};

export const SSOSettings = () => {
  const { formatMessage } = useIntl();

  const { clearErrors } = useFormContext<SsoFormValues>();
  const { errors } = useFormState();

  const [emailDomain, clientId, clientSecret, discoveryEndpoint, subdomain] = useWatch({ name: SSO_FIELDS });

  const isSSOConfigured = useMemo(() => {
    return emailDomain && clientId && clientSecret && discoveryEndpoint && subdomain;
  }, [emailDomain, clientId, clientSecret, discoveryEndpoint, subdomain]);

  const allSSOFieldsAreEmpty = useMemo(() => {
    return !emailDomain && !clientId && !clientSecret && !discoveryEndpoint && !subdomain;
  }, [emailDomain, clientId, clientSecret, discoveryEndpoint, subdomain]);

  useEffect(() => {
    const hasErrors = SSO_FIELDS.some((field) => field in errors);

    if (hasErrors && allSSOFieldsAreEmpty) {
      clearErrors(SSO_FIELDS);
    }
  }, [clearErrors, errors, allSSOFieldsAreEmpty]);

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
                  {isSSOConfigured ? (
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
                    <Box mt="md">
                      <FormControl<SsoFormValues>
                        label={formatMessage({ id: "settings.organizationSettings.sso.emailDomain" })}
                        fieldType="input"
                        name="emailDomain"
                        required
                      />
                      <FormControl<SsoFormValues>
                        label={formatMessage({ id: "settings.organizationSettings.sso.clientId" })}
                        fieldType="input"
                        name="clientId"
                        required
                      />
                      <FormControl<SsoFormValues>
                        label={formatMessage({ id: "settings.organizationSettings.sso.clientSecret" })}
                        fieldType="input"
                        type="password"
                        name="clientSecret"
                        required
                      />
                      <FormControl<SsoFormValues>
                        label={formatMessage({ id: "settings.organizationSettings.sso.discoveryEndpoint" })}
                        fieldType="input"
                        name="discoveryEndpoint"
                        required
                      />
                      <FormControl<SsoFormValues>
                        label={formatMessage({ id: "settings.organizationSettings.sso.subdomain" })}
                        fieldType="input"
                        name="subdomain"
                        required
                      />
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

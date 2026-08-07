import { useFormState } from "react-hook-form";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { FormControl } from "components/ui/forms/FormControl";
import { FormSubmissionButtons } from "components/ui/forms/FormSubmissionButtons";
import { Icon } from "components/ui/Icon";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { useSSOConfigManagement } from "core/api";
import { links } from "core/utils/links";

import { CollapsibleSettingsCard } from "./CollapsibleSettingsCard";
import { SSOFormValues } from "../UpdateSSOSettingsForm";

export const SSOSettings = () => {
  const { formatMessage } = useIntl();
  const { isSSOConfigured, isLoading } = useSSOConfigManagement();
  const { isSubmitting } = useFormState();

  return (
    <CollapsibleSettingsCard
      label={formatMessage({ id: "settings.organizationSettings.sso.label" })}
      docsLink={links.ssoDocs}
      docsLinkLabel={formatMessage({ id: "settings.organizationSettings.sso.docsLink" })}
      status={
        isLoading || isSubmitting ? (
          <Icon type="loading" />
        ) : isSSOConfigured ? (
          <Icon type="check" color="primary" />
        ) : (
          <Text size="sm" color="grey300" italicized>
            {formatMessage({ id: "settings.organizationSettings.sso.label.optional" })}
          </Text>
        )
      }
    >
      {isSSOConfigured ? (
        <FlexContainer direction="column" alignItems="flex-start">
          <Message text={formatMessage({ id: "settings.organizationSettings.sso.configured" })} />
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
    </CollapsibleSettingsCard>
  );
};

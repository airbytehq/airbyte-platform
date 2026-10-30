import { useIntl } from "react-intl";

import { SegmentedControl } from "components/ui/SegmentedControl";

import { ScimIdpProvider } from "core/api/types/AirbyteClient";

interface ScimIdpProviderSelectorProps {
  value?: ScimIdpProvider;
  onChange: (value: ScimIdpProvider) => void;
  disabled?: boolean;
  className?: string;
}

const PROVIDER_OPTIONS: ScimIdpProvider[] = [ScimIdpProvider.okta, ScimIdpProvider.microsoft_entra_id];

const PROVIDER_LABEL_IDS: Record<ScimIdpProvider, string> = {
  [ScimIdpProvider.okta]: "settings.organizationSettings.scim.idpProvider.okta",
  [ScimIdpProvider.microsoft_entra_id]: "settings.organizationSettings.scim.idpProvider.microsoftEntraId",
};

/**
 * Segmented control for choosing the SCIM identity provider ("Okta" | "Microsoft Entra ID").
 *
 * `value` is optional so that no vendor renders pre-selected. Labels and the group's accessible
 * name are resolved internally from i18n keys - callers only pass/receive the wire value.
 */
export const ScimIdpProviderSelector: React.FC<ScimIdpProviderSelectorProps> = ({
  value,
  onChange,
  disabled,
  className,
}) => {
  const { formatMessage } = useIntl();

  return (
    <SegmentedControl<ScimIdpProvider>
      options={PROVIDER_OPTIONS.map((provider) => ({
        value: provider,
        label: formatMessage({ id: PROVIDER_LABEL_IDS[provider] }),
      }))}
      selectedValue={value}
      onSelect={onChange}
      isDisabled={disabled}
      aria-label={formatMessage({ id: "settings.organizationSettings.scim.idpProvider.label" })}
      className={className}
    />
  );
};

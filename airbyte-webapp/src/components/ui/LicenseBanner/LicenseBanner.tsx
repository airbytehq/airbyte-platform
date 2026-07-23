import dayjs from "dayjs";
import { FormattedMessage } from "react-intl";

import { AlertBanner } from "components/ui/Banner/AlertBanner";

import { useGetInstanceConfiguration } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";

export const LicenseBanner: React.FC = () => {
  const showLicenseResults = useFeature(FeatureItem.EnterpriseLicenseChecking);
  const { licenseStatus, licenseExpirationDate } = useGetInstanceConfiguration();
  const daysUntilExpiration = dayjs((licenseExpirationDate ?? 0) * 1000).diff(dayjs(), "days");
  const expiresSoon = daysUntilExpiration <= 30 && daysUntilExpiration >= 0;

  const licenseBannerMessage =
    licenseStatus === "expired"
      ? "settings.license.banner.expired"
      : licenseStatus === "exceeded"
      ? "settings.license.banner.exceeded"
      : licenseStatus === "invalid"
      ? "settings.license.banner.invalid"
      : expiresSoon
      ? "settings.license.banner.expiresSoon"
      : undefined;

  const licenseBannerValues = expiresSoon
    ? { daysLeft: dayjs((licenseExpirationDate ?? 0) * 1000).diff(dayjs(), "days") }
    : undefined;

  if (!licenseBannerMessage || !showLicenseResults) {
    return null;
  }

  return (
    <AlertBanner
      message={<FormattedMessage id={licenseBannerMessage} values={licenseBannerValues} />}
      data-testid="workspace-status-banner"
      color="error"
    />
  );
};

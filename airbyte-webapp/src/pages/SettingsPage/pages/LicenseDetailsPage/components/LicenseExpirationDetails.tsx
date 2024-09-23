import { FormattedDate, FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useGetInstanceConfiguration } from "core/api";

import styles from "./LicenseExpirationDetails.module.scss";
export const LicenseExpirationDetails: React.FC = () => {
  const { licenseStatus, licenseExpirationDate } = useGetInstanceConfiguration();

  if (licenseStatus === "invalid" || !licenseExpirationDate) {
    return (
      <FlexContainer direction="row" alignItems="center" gap="xs">
        <FlexContainer className={styles.expiredLicensePill} alignItems="center">
          <Icon type="warningOutline" size="sm" color="error" />
          <Text color="red400" as="span">
            <FormattedMessage id="settings.license.invalidLicense" />
          </Text>
        </FlexContainer>
      </FlexContainer>
    );
  }

  if (licenseStatus === "expired" || licenseStatus === "exceeded") {
    return (
      <FlexContainer direction="row" alignItems="center" gap="xs">
        <FlexContainer className={styles.expiredLicensePill} alignItems="center">
          <Icon type="warningOutline" size="sm" color="error" />
          <Text color="red400" as="span">
            <FormattedMessage
              id={licenseStatus === "expired" ? "settings.license.expired" : "settings.license.exceeded"}
            />
          </Text>
        </FlexContainer>
        <Text size="sm" as="span">
          <FormattedDate value={(licenseExpirationDate ?? 0) * 1000} dateStyle="medium" />
        </Text>
      </FlexContainer>
    );
  }

  return (
    <FlexContainer direction="row" alignItems="center" gap="xs">
      <Text color="grey500" size="sm" as="span">
        <FormattedMessage id="settings.license.expires" />
      </Text>
      <Text size="sm" as="span">
        <FormattedDate value={licenseExpirationDate * 1000} dateStyle="medium" />
      </Text>
    </FlexContainer>
  );
};

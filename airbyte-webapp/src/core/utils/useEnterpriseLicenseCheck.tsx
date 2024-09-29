import dayjs from "dayjs";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { useGetInstanceConfiguration } from "core/api";
import { FeatureItem, useFeature } from "core/services/features";
import { useModalService } from "hooks/services/Modal";

export const useEnterpriseLicenseCheck = () => {
  const { licenseStatus, licenseExpirationDate } = useGetInstanceConfiguration();
  const [searchParams] = useSearchParams();
  const shouldCheckLicense = searchParams.get("checkLicense");
  const daysUntilExpiration = dayjs((licenseExpirationDate ?? 0) * 1000).diff(dayjs(), "days");
  const expiresSoon = daysUntilExpiration <= 30 && daysUntilExpiration >= 0;
  const showLicenseResults = useFeature(FeatureItem.EnterpriseLicenseChecking);
  const { openModal } = useModalService();

  if (showLicenseResults && shouldCheckLicense === "true") {
    const licenseWarningTitle =
      licenseStatus === "expired"
        ? "settings.license.modalTitle.expired"
        : licenseStatus === "exceeded"
        ? "settings.license.modalTitle.exceeded"
        : licenseStatus === "invalid"
        ? "settings.license.modalTitle.invalid"
        : expiresSoon
        ? "settings.license.modalTitle.expiresSoon"
        : undefined;

    const licenseWarningTitleValues = expiresSoon
      ? { daysLeft: dayjs((licenseExpirationDate ?? 0) * 1000).diff(dayjs(), "days") }
      : undefined;

    if (licenseWarningTitle) {
      return openModal({
        title: (
          <Text as="span" size="xl">
            <FormattedMessage id={licenseWarningTitle} values={licenseWarningTitleValues} />
          </Text>
        ),
        allowNavigation: true,
        preventCancel: true,
        size: "md",
        content: ({ onComplete }) => {
          const handleConfirm = () => {
            searchParams.delete("checkLicense");
            onComplete(null);
          };

          return (
            <>
              <ModalBody>
                <Box py="md">
                  <Text>
                    {licenseWarningTitle === "settings.license.modalTitle.expired" ||
                    licenseWarningTitle === "settings.license.modalTitle.expiresSoon" ? (
                      <FormattedMessage id="settings.license.modalBody" />
                    ) : (
                      <FormattedMessage id="settings.license.modalBody.resolve" />
                    )}
                  </Text>
                </Box>
              </ModalBody>
              <ModalFooter>
                <Button onClick={handleConfirm} variant="primary">
                  <FormattedMessage id="settings.license.modalConfirm" />
                </Button>
              </ModalFooter>
            </>
          );
        },
      });
    }
  }

  return null;
};

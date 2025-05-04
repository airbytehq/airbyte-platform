import dayjs from "dayjs";
import { jwtDecode } from "jwt-decode";
import { useCallback, useEffect, useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { useGetInstanceConfiguration } from "core/api";
import { useAuthService } from "core/services/auth";
import { FeatureItem, useFeature } from "core/services/features";
import { useModalService } from "hooks/services/Modal";

import { useLocalStorage } from "./useLocalStorage";

export const useEnterpriseLicenseCheck = () => {
  const { licenseStatus, licenseExpirationDate } = useGetInstanceConfiguration();
  const daysUntilExpiration = dayjs((licenseExpirationDate ?? 0) * 1000).diff(dayjs(), "days");
  const expiresSoon = daysUntilExpiration <= 30 && daysUntilExpiration >= 0;
  const shouldCheckEnterpriseLicense = useFeature(FeatureItem.EnterpriseLicenseChecking);
  const { openModal } = useModalService();
  const auth = useAuthService();
  const [dismissedAt, setDismissedAt] = useLocalStorage("airbyte_license-check-dismissed-at", null);

  const licenseWarningTitle = useMemo(() => {
    if (licenseStatus === "expired") {
      return "settings.license.modalTitle.expired";
    } else if (licenseStatus === "exceeded") {
      return "settings.license.modalTitle.exceeded";
    } else if (licenseStatus === "invalid") {
      return "settings.license.modalTitle.invalid";
    } else if (expiresSoon) {
      return "settings.license.modalTitle.expiresSoon";
    }
    return undefined;
  }, [licenseStatus, expiresSoon]);

  const licenseWarningTitleValues = useMemo(() => {
    if (expiresSoon) {
      return { daysLeft: dayjs((licenseExpirationDate ?? 0) * 1000).diff(dayjs(), "days") };
    }
    return undefined;
  }, [expiresSoon, licenseExpirationDate]);

  const showLicenseWarning = useCallback(() => {
    openModal({
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
          setDismissedAt(dayjs().toISOString());
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
  }, [licenseWarningTitle, licenseWarningTitleValues, openModal, setDismissedAt]);

  useEffect(() => {
    auth.getAccessToken?.().then((token) => {
      if (!shouldCheckEnterpriseLicense || !expiresSoon || !token) {
        return;
      }

      const lastDismissedAt = dismissedAt ? dayjs(dismissedAt) : null;

      // An OIDC provider may include the auth_time, but it is not required: https://openid.net/specs/openid-connect-core-1_0.html
      const decoded = jwtDecode<{ auth_time?: number }>(token);
      if (decoded.auth_time) {
        const authTime = dayjs.unix(decoded.auth_time);
        if (!lastDismissedAt || authTime.isAfter(lastDismissedAt)) {
          showLicenseWarning();
        }
        // If the OIDC provider does not tell us when the login happened, we will show it max once per day
      } else if (!lastDismissedAt) {
        showLicenseWarning();
      } else if (Math.abs(lastDismissedAt.diff(dayjs(), "day")) >= 1) {
        showLicenseWarning();
      }
    });
  }, [auth, shouldCheckEnterpriseLicense, showLicenseWarning, dismissedAt, expiresSoon]);
};

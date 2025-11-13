import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useCheckDomainVerification } from "core/api";
import { DomainVerificationResponse, DomainVerificationResponseStatus } from "core/api/types/AirbyteClient";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./DomainVerification.module.scss";

interface DomainVerificationItemProps {
  domain: DomainVerificationResponse;
  onViewDnsInfo: (domain: DomainVerificationResponse) => void;
}

const getStatusBadgeVariant = (status: DomainVerificationResponseStatus): "green" | "yellow" | "red" | "grey" => {
  switch (status) {
    case "VERIFIED":
      return "green";
    case "PENDING":
      return "yellow";
    case "FAILED":
      return "red";
    case "EXPIRED":
      return "grey";
    default:
      return "grey";
  }
};

const MINIMUM_LOADING_DELAY = 800; // milliseconds

export const DomainVerificationItem: React.FC<DomainVerificationItemProps> = ({ domain, onViewDnsInfo }) => {
  const { mutateAsync: checkNow } = useCheckDomainVerification();
  const { registerNotification } = useNotificationService();
  const [isChecking, setIsChecking] = useState(false);

  const formatDate = (timestamp?: number | null) => {
    if (!timestamp) {
      return null;
    }
    // Convert to milliseconds
    return new Date(timestamp * 1000).toLocaleDateString();
  };

  const handleCheckNow = async () => {
    setIsChecking(true);
    try {
      // Ensure a minimum loading time to prevent jarring UI flicker
      await Promise.all([checkNow(domain.id), new Promise((resolve) => setTimeout(resolve, MINIMUM_LOADING_DELAY))]);
    } catch (error) {
      registerNotification({
        id: "domain-verification-check-error",
        text: <FormattedMessage id="settings.organizationSettings.domainVerification.checkError" />,
        type: "error",
      });
    } finally {
      setIsChecking(false);
    }
  };

  return (
    <div className={styles.domainItem}>
      <FlexContainer justifyContent="space-between" alignItems="center">
        <FlexContainer direction="column" gap="md">
          <Text bold size="lg">
            {domain.domain}
          </Text>

          <FlexContainer gap="md" alignItems="center">
            <Badge variant={getStatusBadgeVariant(domain.status)}>
              <FormattedMessage id={`settings.organizationSettings.domainVerification.status.${domain.status}`} />
            </Badge>
            {domain.verifiedAt ? (
              <Text size="sm" color="grey">
                <FormattedMessage
                  id="settings.organizationSettings.domainVerification.verifiedAt"
                  values={{ date: formatDate(domain.verifiedAt) }}
                />
              </Text>
            ) : domain.status === "PENDING" ? (
              <Text size="sm" color="grey" italicized>
                <FormattedMessage id="settings.organizationSettings.domainVerification.pendingMessage" />
              </Text>
            ) : null}
          </FlexContainer>
        </FlexContainer>

        <FlexContainer gap="sm">
          <Button variant="secondary" size="sm" icon="eye" onClick={() => onViewDnsInfo(domain)}>
            <FormattedMessage id="settings.organizationSettings.domainVerification.viewDnsInfo" />
          </Button>
          {domain.status === "PENDING" && (
            <Button variant="secondary" size="sm" icon="rotate" onClick={handleCheckNow} isLoading={isChecking}>
              <FormattedMessage id="settings.organizationSettings.domainVerification.checkNow" />
            </Button>
          )}
        </FlexContainer>
      </FlexContainer>
    </div>
  );
};

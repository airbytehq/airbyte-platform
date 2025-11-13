import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { DomainVerificationResponse } from "core/api/types/AirbyteClient";

import { DomainVerificationItem } from "./DomainVerificationItem";

interface DomainVerificationListProps {
  domains: DomainVerificationResponse[];
  isLoading: boolean;
  onViewDnsInfo: (domain: DomainVerificationResponse) => void;
}

export const DomainVerificationList: React.FC<DomainVerificationListProps> = ({
  domains,
  isLoading,
  onViewDnsInfo,
}) => {
  if (isLoading) {
    return (
      <FlexContainer justifyContent="center">
        <Text color="grey">
          <FormattedMessage id="settings.organizationSettings.domainVerification.loading" />
        </Text>
      </FlexContainer>
    );
  }

  if (domains.length === 0) {
    return (
      <FlexContainer justifyContent="center">
        <Text color="grey">
          <FormattedMessage id="settings.organizationSettings.domainVerification.noDomains" />
        </Text>
      </FlexContainer>
    );
  }

  return (
    <FlexContainer direction="column" gap="md">
      {domains.map((domain) => (
        <DomainVerificationItem key={domain.id} domain={domain} onViewDnsInfo={onViewDnsInfo} />
      ))}
    </FlexContainer>
  );
};

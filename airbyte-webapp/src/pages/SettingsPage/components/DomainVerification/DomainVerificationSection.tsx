import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useListDomainVerifications } from "core/api";
import { DomainVerificationResponse } from "core/api/types/AirbyteClient";
import { useIntent } from "core/utils/rbac";

import styles from "./DomainVerification.module.scss";
import { DomainVerificationList } from "./DomainVerificationList";
import { DomainVerificationModal } from "./DomainVerificationModal";

export const DomainVerificationSection: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const canUpdateOrganization = useIntent("UpdateOrganization", { organizationId });
  const { data, isLoading } = useListDomainVerifications();

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedDomain, setSelectedDomain] = useState<DomainVerificationResponse | undefined>(undefined);

  const domains = data?.domainVerifications || [];

  const handleViewDnsInfo = (domain: DomainVerificationResponse) => {
    setSelectedDomain(domain);
    setIsModalOpen(true);
  };

  const handleAddNewDomain = () => {
    setSelectedDomain(undefined);
    setIsModalOpen(true);
  };

  const handleCloseModal = () => {
    setIsModalOpen(false);
    setSelectedDomain(undefined);
  };

  return (
    <div className={styles.section}>
      <FlexContainer direction="column" gap="lg">
        <FlexContainer alignItems="center" justifyContent="space-between">
          <FlexItem grow>
            <Heading as="h2" size="sm">
              <FormattedMessage id="settings.organizationSettings.domainVerification.title" />
            </Heading>
          </FlexItem>
          <Button
            variant="primary"
            size="sm"
            icon="plus"
            onClick={handleAddNewDomain}
            disabled={!canUpdateOrganization}
          >
            <FormattedMessage id="settings.organizationSettings.domainVerification.addDomain" />
          </Button>
        </FlexContainer>

        <Text color="grey">
          <FormattedMessage id="settings.organizationSettings.domainVerification.description" />
        </Text>

        <DomainVerificationList domains={domains} isLoading={isLoading} onViewDnsInfo={handleViewDnsInfo} />

        {isModalOpen && <DomainVerificationModal onClose={handleCloseModal} existingDomain={selectedDomain} />}
      </FlexContainer>
    </div>
  );
};

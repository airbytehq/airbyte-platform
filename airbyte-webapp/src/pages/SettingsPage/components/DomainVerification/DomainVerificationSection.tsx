import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useListDomainVerifications } from "core/api";
import { useIntent } from "core/utils/rbac";

import { AddDomainModal } from "./AddDomainModal";
import styles from "./DomainVerification.module.scss";
import { DomainVerificationList } from "./DomainVerificationList";

export const DomainVerificationSection: React.FC = () => {
  const organizationId = useCurrentOrganizationId();
  const canUpdateOrganization = useIntent("UpdateOrganization", { organizationId });
  const { data, isLoading } = useListDomainVerifications();

  const [isModalOpen, setIsModalOpen] = useState(false);

  const domains = data?.domainVerifications || [];

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
            onClick={() => setIsModalOpen(true)}
            disabled={!canUpdateOrganization}
          >
            <FormattedMessage id="settings.organizationSettings.domainVerification.addDomain" />
          </Button>
        </FlexContainer>

        <Text color="grey">
          <FormattedMessage id="settings.organizationSettings.domainVerification.description" />
        </Text>

        <DomainVerificationList domains={domains} isLoading={isLoading} />

        {isModalOpen && <AddDomainModal onClose={() => setIsModalOpen(false)} />}
      </FlexContainer>
    </div>
  );
};

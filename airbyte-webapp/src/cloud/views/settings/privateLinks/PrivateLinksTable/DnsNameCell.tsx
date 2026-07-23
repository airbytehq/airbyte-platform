import React from "react";

import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { PrivateLinkStatus } from "core/api/types/AirbyteClient";

import styles from "./DnsNameCell.module.scss";

const DNS_VISIBLE_STATUSES: ReadonlySet<PrivateLinkStatus> = new Set([
  PrivateLinkStatus.available,
  PrivateLinkStatus.deleting,
  PrivateLinkStatus.delete_failed,
]);

interface DnsNameCellProps {
  dnsName: string | undefined;
  status: PrivateLinkStatus;
}

export const DnsNameCell: React.FC<DnsNameCellProps> = ({ dnsName, status }) => {
  if (dnsName && DNS_VISIBLE_STATUSES.has(status)) {
    return (
      <FlexContainer alignItems="center" gap="sm" className={styles.dnsCell}>
        <Text size="sm" className={styles.dnsText}>
          {dnsName}
        </Text>
        <CopyButton content={dnsName} variant="clear" />
      </FlexContainer>
    );
  }

  return (
    <Text size="sm" color="grey">
      —
    </Text>
  );
};

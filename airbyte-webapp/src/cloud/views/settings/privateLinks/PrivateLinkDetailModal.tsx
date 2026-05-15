import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer } from "components/ui/Flex";
import { ModalBody } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { PrivateLinkRead } from "core/api/types/AirbyteClient";

import styles from "./PrivateLinkDetailModal.module.scss";

interface DetailRowProps {
  label: string;
  copyable?: string;
  children: React.ReactNode;
}

const DetailRow: React.FC<DetailRowProps> = ({ label, copyable, children }) => (
  <FlexContainer direction="column" gap="xs">
    <Text size="xs" color="grey" bold>
      <FormattedMessage id={label} />
    </Text>
    <FlexContainer alignItems="center" gap="sm" className={styles.detailRow}>
      {children}
      {copyable && <CopyButton content={copyable} variant="clear" />}
    </FlexContainer>
  </FlexContainer>
);

interface PrivateLinkDetailModalProps {
  link: PrivateLinkRead;
}

export const PrivateLinkDetailModal: React.FC<PrivateLinkDetailModalProps> = ({ link }) => {
  const { formatMessage } = useIntl();

  return (
    <ModalBody>
      <FlexContainer direction="column" gap="lg">
        <DetailRow label="settings.privateLinks.details.id" copyable={link.id}>
          <Text size="sm">{link.id}</Text>
        </DetailRow>

        <DetailRow label="settings.privateLinks.table.name">
          <Text size="sm">{link.name}</Text>
        </DetailRow>

        <DetailRow label="settings.privateLinks.details.status">
          <Text size="sm">{formatMessage({ id: `settings.privateLinks.status.${link.status}` })}</Text>
        </DetailRow>

        <DetailRow label="settings.privateLinks.details.serviceName" copyable={link.serviceName}>
          <Text size="sm">{link.serviceName}</Text>
        </DetailRow>

        <DetailRow label="settings.privateLinks.details.serviceRegion">
          <Text size="sm">{link.serviceRegion}</Text>
        </DetailRow>

        <DetailRow label="settings.privateLinks.details.endpointId" copyable={link.endpointId}>
          <Text size="sm">{link.endpointId ?? "—"}</Text>
        </DetailRow>

        <DetailRow label="settings.privateLinks.details.dnsName" copyable={link.dnsName}>
          <Text size="sm" className={styles.dnsName}>
            {link.dnsName ?? "—"}
          </Text>
        </DetailRow>
      </FlexContainer>
    </ModalBody>
  );
};

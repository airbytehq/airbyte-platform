import React from "react";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { SupportLevelBadge } from "components/ui/SupportLevelBadge";

import { ConnectorDefinition } from "core/domain/connector";

import styles from "./ConnectorQualityMetrics.module.scss";

interface MetadataStatProps {
  label: string;
}

const MetadataStat: React.FC<React.PropsWithChildren<MetadataStatProps>> = ({ label, children }) => (
  <FlexContainer direction="row" alignItems="center" gap="md" className={styles.metadataStat}>
    <FlexContainer className={styles.metadataStatLabel}>{`${label}:`}</FlexContainer>
    <FlexContainer className={styles.metadataStatValue}>{children}</FlexContainer>
  </FlexContainer>
);

interface ConnectorQualityMetricsProps {
  connectorDefinition: ConnectorDefinition;
}

export const ConnectorQualityMetrics: React.FC<ConnectorQualityMetricsProps> = ({ connectorDefinition }) => {
  const { formatMessage } = useIntl();

  return (
    <FlexContainer direction="column" gap="sm" className={styles.connectorMetadata}>
      <MetadataStat label={formatMessage({ id: "docs.metrics.supportLevel.label" })}>
        <SupportLevelBadge supportLevel={connectorDefinition.supportLevel} className={styles.statChip} />
      </MetadataStat>
      <MetadataStat label={formatMessage({ id: "docs.metrics.connectorVersion.label" })}>
        <a href="#changelog">{connectorDefinition.dockerImageTag}</a>
      </MetadataStat>
    </FlexContainer>
  );
};

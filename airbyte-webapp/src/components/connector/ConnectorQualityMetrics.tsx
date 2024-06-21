import dayjs from "dayjs";
import React from "react";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon, IconType } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link/ExternalLink";
import { SupportLevelBadge } from "components/ui/SupportLevelBadge";

import { usePythonCDKVersion } from "core/api";
import { ConnectorDefinition } from "core/domain/connector";

import styles from "./ConnectorQualityMetrics.module.scss";

interface CDKVersion {
  version?: string;
  isLatest: boolean;
  url?: string;
  language: string;
}

const parseCDKVersion = (connectorCdkVersion?: string, latestPythonCdkVersion?: string): CDKVersion => {
  if (!connectorCdkVersion || !connectorCdkVersion.includes(":")) {
    return { version: connectorCdkVersion, isLatest: false, language: "unknown" };
  }

  const [language, version] = connectorCdkVersion.split(":");
  switch (language) {
    case "python":
      const isLatest = version === latestPythonCdkVersion;
      const packageUrl = `https://pypi.org/project/airbyte-cdk/${version}/`;
      return { version, isLatest, url: packageUrl, language };
    case "java":
      // TODO: add java cdk latest version check
      // Note as of June 2024 we do not report java cdk version per connector
      return { version, isLatest: true, language };
    default:
      return { version, isLatest: false, language };
  }
};

interface MetricInfo {
  icon: IconType;
  title: string;
}
type MetricLevel = "high" | "medium" | "low";
type IconMap = Record<MetricLevel, MetricInfo>;

const USAGE_ICON_MAP: IconMap = {
  high: {
    icon: "metricUsageHigh",
    title: "docs.metrics.usageRate.tooltip.high",
  },
  medium: {
    icon: "metricUsageMed",
    title: "docs.metrics.usageRate.tooltip.med",
  },
  low: {
    icon: "metricUsageLow",
    title: "docs.metrics.usageRate.tooltip.low",
  },
} as const;

const SUCCESS_ICON_MAP: IconMap = {
  high: {
    icon: "metricSuccessHigh",
    title: "docs.metrics.syncSuccessRate.tooltip.high",
  },
  medium: {
    icon: "metricSuccessMed",
    title: "docs.metrics.syncSuccessRate.tooltip.med",
  },
  low: {
    icon: "metricSuccessLow",
    title: "docs.metrics.syncSuccessRate.tooltip.low",
  },
} as const;

interface MetricIconProps {
  iconMap: IconMap;
  level: MetricLevel;
}

const MetricIcon: React.FC<MetricIconProps> = ({ iconMap, level }) => {
  const { formatMessage } = useIntl();

  const lowercaseLevel = level.toLowerCase() as MetricLevel;
  if (!iconMap[lowercaseLevel]) {
    return null;
  }

  const { icon, title } = iconMap[lowercaseLevel];
  return (
    <Icon
      className={styles.wideIcon}
      size="xs"
      type={icon}
      title={formatMessage({ id: title })}
      aria-label={formatMessage({ id: title })}
    />
  );
};

interface MetadataStatProps {
  label: string;
}

const MetadataStat: React.FC<React.PropsWithChildren<MetadataStatProps>> = ({ label, children }) => (
  <FlexContainer direction="row" alignItems="center" gap="sm" className={styles.metadataStat}>
    <FlexContainer className={styles.metadataStatLabel}>{`${label}:`}</FlexContainer>
    <FlexContainer className={styles.metadataStatValue} gap="sm">
      {children}
    </FlexContainer>
  </FlexContainer>
);

interface ConnectorQualityMetricsProps {
  connectorDefinition: ConnectorDefinition;
}

export const ConnectorQualityMetrics: React.FC<ConnectorQualityMetricsProps> = ({ connectorDefinition }) => {
  const { formatMessage } = useIntl();

  const rawCDKVersion = connectorDefinition?.cdkVersion;
  const lastPublished = connectorDefinition?.lastPublished;
  const syncSuccessRate = connectorDefinition?.metrics?.all?.sync_success_rate;
  const usageRate = connectorDefinition?.metrics?.all?.usage;
  const latestPythonCdkVersion = usePythonCDKVersion();
  const {
    version: cdkVersion,
    isLatest: isLatestCDK,
    url: cdkVersionUrl,
  } = parseCDKVersion(rawCDKVersion, latestPythonCdkVersion);

  return (
    <FlexContainer direction="column" gap="sm" className={styles.connectorMetadata}>
      <MetadataStat label={formatMessage({ id: "docs.metrics.supportLevel.label" })}>
        <SupportLevelBadge supportLevel={connectorDefinition.supportLevel} className={styles.statChip} />
      </MetadataStat>
      <MetadataStat label={formatMessage({ id: "docs.metrics.connectorVersion.label" })}>
        <a href="#changelog">{connectorDefinition.dockerImageTag}</a>
        {lastPublished && (
          <span className={styles.deemphasizeText}>{`(${formatMessage({
            id: "docs.metrics.lastPublished.label",
          })} ${dayjs(lastPublished).fromNow()})`}</span>
        )}
      </MetadataStat>

      {cdkVersion && (
        <MetadataStat label={formatMessage({ id: "docs.metrics.cdkVersion.label" })}>
          {cdkVersionUrl ? (
            <ExternalLink variant="primary" opensInNewTab href={cdkVersionUrl}>
              {cdkVersion}
            </ExternalLink>
          ) : (
            cdkVersion
          )}
          {isLatestCDK && (
            <span className={styles.deemphasizeText}>{`(${formatMessage({
              id: "docs.metrics.isLatestCDK.label",
            })})`}</span>
          )}
        </MetadataStat>
      )}
      {syncSuccessRate && (
        <MetadataStat label={formatMessage({ id: "docs.metrics.syncSuccessRate.label" })}>
          <MetricIcon iconMap={SUCCESS_ICON_MAP} level={syncSuccessRate} />
        </MetadataStat>
      )}
      {usageRate && (
        <MetadataStat label={formatMessage({ id: "docs.metrics.usageRate.label" })}>
          <MetricIcon iconMap={USAGE_ICON_MAP} level={usageRate} />
        </MetadataStat>
      )}
    </FlexContainer>
  );
};

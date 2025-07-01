import classNames from "classnames";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { BorderedTile, BorderedTiles } from "components/ui/BorderedTiles";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import { useGetLicenseDetails } from "core/api";

import { LicenseExpirationDetails } from "./components/LicenseExpirationDetails";
import styles from "./LicenseSettingsPage.module.scss";

const LicenseTile: React.FC<{
  type: "nodes" | "editors";
  maxValue?: number;
  usedValue?: number;
  className: string;
}> = ({ type, maxValue, usedValue, className }) => {
  const titleId = type === "nodes" ? "settings.license.nodes" : "settings.license.editors";
  const tooltipId = type === "nodes" ? "settings.license.nodesTooltip" : "settings.license.editorsTooltip";

  const tileContent = useMemo(() => {
    if (usedValue && maxValue) {
      return `${usedValue}/${maxValue}`;
    } else if (usedValue) {
      return <FormattedMessage id="settings.license.inUse" values={{ count: usedValue }} />;
    } else if (maxValue) {
      return <FormattedMessage id="settings.license.allowed" values={{ count: maxValue }} />;
    }
    return null;
  }, [maxValue, usedValue]);

  if (!tileContent) {
    return null;
  }

  return (
    <BorderedTile className={className}>
      <Heading as="h2" size="sm">
        <FormattedMessage id={titleId} />
        <InfoTooltip>
          <FormattedMessage id={tooltipId} />
        </InfoTooltip>
      </Heading>

      <Heading as="h3">{tileContent}</Heading>
    </BorderedTile>
  );
};

export const LicenseSettingsPage: React.FC = () => {
  const licenseData = useGetLicenseDetails();

  const nodesClassname = classNames({
    [styles.error]: !!licenseData.maxNodes && !!licenseData.usedNodes && licenseData.maxNodes < licenseData.usedNodes,
  });

  const editorsClassname = classNames({
    [styles.error]: !!licenseData.maxEditors && licenseData.maxEditors < licenseData.usedEditors,
  });

  const NodesTile = () => (
    <LicenseTile
      type="nodes"
      maxValue={licenseData.maxNodes}
      usedValue={licenseData.usedNodes}
      className={nodesClassname}
    />
  );

  const EditorsTile: React.FC = () => (
    <LicenseTile
      type="editors"
      maxValue={licenseData.maxEditors}
      usedValue={licenseData.usedEditors}
      className={editorsClassname}
    />
  );

  return (
    <FlexContainer direction="column" gap="lg">
      <div>
        <Heading as="h1" size="md">
          <FormattedMessage id="settings.license" />
        </Heading>
      </div>
      <LicenseExpirationDetails />
      <BorderedTiles>
        <BorderedTile>
          <Heading as="h2" size="sm">
            <FormattedMessage id="settings.license.plan" />
          </Heading>
          <Text size="lg">
            <FormattedMessage id="settings.license.plan.selfManagedEnterprise" />
          </Text>
        </BorderedTile>
        <EditorsTile />
        <NodesTile />
      </BorderedTiles>
    </FlexContainer>
  );
};

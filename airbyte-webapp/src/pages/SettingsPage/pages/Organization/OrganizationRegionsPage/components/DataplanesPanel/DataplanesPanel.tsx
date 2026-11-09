import React from "react";
import { FormattedMessage } from "react-intl";

import { EmptyState } from "components/ui/EmptyState";
import { FlexContainer } from "components/ui/Flex";
import AirbyteLogoIcon from "components/ui/illustrations/airbyte-logo-icon.svg?react";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { links } from "core/utils/links";

import styles from "./DataplanesPanel.module.scss";
import { Region } from "../../types";
import { DataplaneHealthStats } from "../DataplaneHealthStats";

interface DataplanesPanelProps {
  region: Region;
}

export const DataplanesPanel: React.FC<DataplanesPanelProps> = ({ region }) => {
  if (region.isAirbyteManaged) {
    return (
      <FlexContainer alignItems="center" justifyContent="space-between" className={styles.panel}>
        <FlexContainer alignItems="center" gap="lg">
          <AirbyteLogoIcon className={styles.logo} />
          <Text size="lg">
            <FormattedMessage id="settings.organization.regions.airbyteManaged.message" />
          </Text>
        </FlexContainer>
        <ExternalLink href={links.statusLink} variant="button">
          <FormattedMessage id="settings.organization.regions.airbyteManaged.statusPage" />
        </ExternalLink>
      </FlexContainer>
    );
  }

  if (region.dataplanes.length === 0) {
    return (
      <div className={styles.panel}>
        <EmptyState
          icon="chart"
          text={<FormattedMessage id="nestedRegions.noDataplanes" />}
          description={<FormattedMessage id="settings.organization.regions.noDataplanes.description" />}
        />
      </div>
    );
  }

  return (
    <div>
      {region.dataplanes.map((dataplane) => (
        <DataplaneHealthStats key={dataplane.dataplane_id} dataplane={dataplane} />
      ))}
    </div>
  );
};

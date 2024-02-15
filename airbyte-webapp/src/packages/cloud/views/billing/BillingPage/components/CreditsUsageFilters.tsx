import React from "react";
import { useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { ClearFiltersButton } from "components/ui/ClearFiltersButton";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ListBox, ListBoxControlButtonProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { DestinationId, SourceId } from "core/api/types/AirbyteClient";
import { ConsumptionTimeWindow } from "core/api/types/CloudApi";

import { useCreditsContext } from "./CreditsUsageContext";
import styles from "./CreditsUsageFilters.module.scss";

const CustomControlButton = <T,>({ selectedOption }: ListBoxControlButtonProps<T>) => {
  const { formatMessage } = useIntl();

  return (
    <>
      {selectedOption ? (
        <Text color="grey" bold className={styles.controlButtonLabel}>
          {selectedOption.label}
        </Text>
      ) : (
        <Text as="span" size="lg" color="grey">
          {formatMessage({ id: "form.selectValue" })}
        </Text>
      )}

      <Icon type="caretDown" color="action" />
    </>
  );
};

export const CreditsUsageFilters: React.FC = () => {
  const {
    sourceOptions,
    destinationOptions,
    setSelectedDestination,
    setSelectedSource,
    setSelectedTimeWindow,
    selectedDestination,
    selectedSource,
    selectedTimeWindow,
  } = useCreditsContext();

  const hasAnyFilterSelected = [selectedSource, selectedDestination].some((selection) => !!selection);

  const onSourceSelect = (currentSourceOption: SourceId | null) => {
    setSelectedSource(currentSourceOption);
  };

  const onDestinationSelect = (currentDestinationOption: DestinationId | null) => {
    setSelectedDestination(currentDestinationOption);
  };

  return (
    <Box px="lg">
      <FlexContainer>
        <ListBox
          controlButton={CustomControlButton}
          buttonClassName={styles.controlButton}
          optionsMenuClassName={styles.periodOptionsMenu}
          options={[
            { label: "Last 30 Days", value: ConsumptionTimeWindow.lastMonth },
            { label: "Last 6 months", value: ConsumptionTimeWindow.lastSixMonths },
            { label: "Last 12 months", value: ConsumptionTimeWindow.lastYear },
          ]}
          selectedValue={selectedTimeWindow}
          onSelect={(selectedValue) => setSelectedTimeWindow(selectedValue)}
        />
        <ListBox
          className={styles.listboxContainer}
          controlButton={CustomControlButton}
          buttonClassName={styles.controlButton}
          optionsMenuClassName={styles.connectorOptionsMenu}
          options={[{ label: "All Sources", value: null }, ...sourceOptions]}
          selectedValue={selectedSource}
          onSelect={(selectedValue) => onSourceSelect(selectedValue)}
        />
        <ListBox
          className={styles.listboxContainer}
          controlButton={CustomControlButton}
          buttonClassName={styles.controlButton}
          optionsMenuClassName={styles.connectorOptionsMenu}
          options={[{ label: "All Destinations", value: null }, ...destinationOptions]}
          selectedValue={selectedDestination}
          onSelect={(selectedValue) => onDestinationSelect(selectedValue)}
        />
        {hasAnyFilterSelected && (
          <ClearFiltersButton
            onClick={() => {
              onSourceSelect(null);
              onDestinationSelect(null);
            }}
          />
        )}
      </FlexContainer>
    </Box>
  );
};

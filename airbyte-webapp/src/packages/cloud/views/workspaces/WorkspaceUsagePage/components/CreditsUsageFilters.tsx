import React from "react";
import { useIntl } from "react-intl";

import { ClearFiltersButton } from "components/ui/ClearFiltersButton";
import { FlexContainer } from "components/ui/Flex";
import { ListBox, ListBoxControlButtonProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { ConsumptionTimeWindow, DestinationId, SourceId } from "core/api/types/AirbyteClient";

import { useCreditsContext } from "./CreditsUsageContext";
import styles from "./CreditsUsageFilters.module.scss";

const CustomControlButton = <T,>({ selectedOption }: ListBoxControlButtonProps<T>) => {
  const { formatMessage } = useIntl();

  return (
    <>
      {selectedOption ? (
        <Text color="grey" bold>
          {selectedOption.label}
        </Text>
      ) : (
        <Text as="span" size="lg" color="grey">
          {formatMessage({ id: "form.selectValue" })}
        </Text>
      )}
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
  const { formatMessage } = useIntl();
  const hasAnyFilterSelected = [selectedSource, selectedDestination].some((selection) => !!selection);

  const onSourceSelect = (currentSourceOption: SourceId | null) => {
    setSelectedSource(currentSourceOption);
  };

  const onDestinationSelect = (currentDestinationOption: DestinationId | null) => {
    setSelectedDestination(currentDestinationOption);
  };

  return (
    <FlexContainer>
      <ListBox
        controlButtonContent={CustomControlButton}
        buttonClassName={styles.controlButton}
        options={[
          {
            label: formatMessage({ id: "settings.organization.billing.filter.lastThirtyDays" }),
            value: ConsumptionTimeWindow.lastMonth,
          },
          {
            label: formatMessage({ id: "settings.organization.billing.filter.lastSixMonths" }),
            value: ConsumptionTimeWindow.lastSixMonths,
          },
          {
            label: formatMessage({ id: "settings.organization.billing.filter.lastTwelveMonths" }),
            value: ConsumptionTimeWindow.lastYear,
          },
        ]}
        selectedValue={selectedTimeWindow}
        onSelect={(selectedValue) => setSelectedTimeWindow(selectedValue)}
      />
      <ListBox
        controlButtonContent={CustomControlButton}
        buttonClassName={styles.controlButton}
        options={[
          { label: formatMessage({ id: "settings.organization.billing.filter.allSources" }), value: null },
          ...sourceOptions,
        ]}
        selectedValue={selectedSource}
        onSelect={(selectedValue) => onSourceSelect(selectedValue)}
      />
      <ListBox
        controlButtonContent={CustomControlButton}
        buttonClassName={styles.controlButton}
        options={[
          { label: formatMessage({ id: "settings.organization.billing.filter.allDestinations" }), value: null },
          ...destinationOptions,
        ]}
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
  );
};

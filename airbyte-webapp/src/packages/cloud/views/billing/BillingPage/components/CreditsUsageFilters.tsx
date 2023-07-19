import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ListBox, ListBoxControlButtonProps } from "components/ui/ListBox";
import { ReactComponent as CaretDownIcon } from "components/ui/ListBox/CaretDownIcon.svg";
import { Text } from "components/ui/Text";

import { ConsumptionTimeWindow } from "core/api/types/CloudApi";
import { DestinationId, SourceId } from "core/request/AirbyteClient";

import { useCreditsContext } from "./CreditsUsageContext";
import styles from "./CreditsUsageFilters.module.scss";

const CustomControlButton = <T,>({ selectedOption }: ListBoxControlButtonProps<T>) => {
  const { formatMessage } = useIntl();

  return (
    <>
      {selectedOption ? (
        <div className={styles.controlButtonLabel}>{selectedOption.label}</div>
      ) : (
        <Text as="span" size="lg" color="grey">
          {formatMessage({ id: "form.selectValue" })}
        </Text>
      )}

      <CaretDownIcon className={styles.caret} />
    </>
  );
};

export const CreditsUsageFilters = () => {
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

  const onSourceSelect = (currentSourceOption: SourceId | null) => {
    setSelectedSource(currentSourceOption);
  };

  const onDestinationSelect = (currentDestinationOption: DestinationId | null) => {
    setSelectedDestination(currentDestinationOption);
  };

  return (
    <Box px="lg">
      <FlexContainer>
        <FlexContainer direction="column" gap="xs">
          <Text color="grey" size="sm">
            <FormattedMessage id="credits.timePeriod" />
          </Text>
          <ListBox
            className={styles.listboxContainer}
            controlButton={CustomControlButton}
            options={[
              { label: "Last 30 Days", value: ConsumptionTimeWindow.lastMonth },
              { label: "Last 6 months", value: ConsumptionTimeWindow.lastSixMonths },
              { label: "Last 12 months", value: ConsumptionTimeWindow.lastYear },
            ]}
            selectedValue={selectedTimeWindow}
            onSelect={(selectedValue) => setSelectedTimeWindow(selectedValue)}
          />
        </FlexContainer>

        <FlexContainer direction="column" gap="xs">
          <Text color="grey" size="sm">
            <FormattedMessage id="credits.source" />
          </Text>
          <ListBox
            className={styles.listboxContainer}
            controlButton={CustomControlButton}
            options={[{ label: "All Sources", value: null }, ...sourceOptions]}
            selectedValue={selectedSource}
            onSelect={(selectedValue) => onSourceSelect(selectedValue)}
          />
        </FlexContainer>

        <FlexContainer direction="column" gap="xs">
          <Text color="grey" size="sm">
            <FormattedMessage id="credits.destination" />
          </Text>
          <ListBox
            className={styles.listboxContainer}
            controlButton={CustomControlButton}
            options={[{ label: "All Destinations", value: null }, ...destinationOptions]}
            selectedValue={selectedDestination}
            onSelect={(selectedValue) => onDestinationSelect(selectedValue)}
          />
        </FlexContainer>
      </FlexContainer>
    </Box>
  );
};

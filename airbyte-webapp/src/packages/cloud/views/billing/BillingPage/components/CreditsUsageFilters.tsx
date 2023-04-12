import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { DestinationId, SourceId } from "core/request/AirbyteClient";

import { useCreditsContext } from "./CreditsUsageContext";
import styles from "./CreditsUsageFilters.module.scss";

export const CreditsUsageFilters = () => {
  const {
    sourceOptions,
    destinationOptions,
    setSelectedDestination,
    setSelectedSource,
    selectedDestination,
    selectedSource,
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
            <FormattedMessage id="credits.source" />
          </Text>
          <ListBox
            className={styles.listboxContainer}
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
            options={[{ label: "All Destinations", value: null }, ...destinationOptions]}
            selectedValue={selectedDestination}
            onSelect={(selectedValue) => onDestinationSelect(selectedValue)}
          />
        </FlexContainer>
      </FlexContainer>
    </Box>
  );
};

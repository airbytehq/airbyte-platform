import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { ClearFiltersButton } from "components/ui/ClearFiltersButton";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import styles from "./ConnectionTimelineFilters.module.scss";
import { eventTypeFilterOptions, statusFilterOptions, TimelineFilterValues } from "./utils";

interface ConnectionTimelineFiltersProps {
  filterValues: TimelineFilterValues;
  setFilterValue: (key: keyof TimelineFilterValues, value: string | null) => void;
  resetFilters: () => void;
  filtersAreDefault: boolean;
}

export const ConnectionTimelineFilters: React.FC<ConnectionTimelineFiltersProps> = ({
  filterValues,
  setFilterValue,
  resetFilters,
  filtersAreDefault,
}) => {
  return (
    <FlexContainer gap="sm" alignItems="center">
      {!!filterValues.eventId ? (
        <FlexItem>
          <FlexContainer className={styles.filterButton} alignItems="center">
            <Box px="md">
              <Text color="grey" bold>
                <FormattedMessage id="connection.timeline.filters.eventId" values={{ eventId: filterValues.eventId }} />
              </Text>
            </Box>
          </FlexContainer>
        </FlexItem>
      ) : (
        <>
          <FlexItem>
            <ListBox
              buttonClassName={styles.filterButton}
              optionClassName={styles.filterOption}
              optionTextAs="span"
              options={statusFilterOptions}
              selectedValue={filterValues.status}
              onSelect={(value) => setFilterValue("status", value)}
            />
          </FlexItem>
          <FlexItem>
            <ListBox
              buttonClassName={styles.filterButton}
              optionClassName={styles.filterOption}
              optionTextAs="span"
              options={eventTypeFilterOptions}
              selectedValue={filterValues.eventType}
              onSelect={(value) => setFilterValue("eventType", value)}
            />
          </FlexItem>
        </>
      )}

      {!filtersAreDefault && (
        <FlexItem>
          <ClearFiltersButton onClick={resetFilters} />
        </FlexItem>
      )}
    </FlexContainer>
  );
};

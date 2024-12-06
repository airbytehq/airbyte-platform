import dayjs from "dayjs";
import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { ClearFiltersButton } from "components/ui/ClearFiltersButton";
import { RangeDatePicker } from "components/ui/DatePicker";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { useCurrentConnection } from "core/api";

import styles from "./ConnectionTimelineFilters.module.scss";
import { eventTypeFilterOptions, statusFilterOptions, TimelineFilterValues } from "./utils";

interface ConnectionTimelineFiltersProps {
  filterValues: TimelineFilterValues;
  setFilterValue: (key: keyof TimelineFilterValues, value: string) => void;
  resetFilters: () => void;
  filtersAreDefault: boolean;
}

const EARLIEST_TIMELINE_EVENTS_AVAILABLE_DATE = dayjs("2024-06-20");
const END_OF_TODAY = dayjs().endOf("day").toISOString();

export const ConnectionTimelineFilters: React.FC<ConnectionTimelineFiltersProps> = ({
  filterValues,
  setFilterValue,
  resetFilters,
  filtersAreDefault,
}) => {
  const { createdAt: connectionCreatedAt } = useCurrentConnection();
  const dayConnectionCreatedAt = dayjs(connectionCreatedAt ?? 0).startOf("day");
  const filterStart = EARLIEST_TIMELINE_EVENTS_AVAILABLE_DATE.isAfter(dayConnectionCreatedAt)
    ? EARLIEST_TIMELINE_EVENTS_AVAILABLE_DATE.toISOString()
    : dayConnectionCreatedAt.toISOString();

  const [tempRangeDateFilterValue, setTempRangeDateFilterValue] = useState<{ startDate: string; endDate: string }>({
    startDate: filterValues.startDate,
    endDate: filterValues.endDate,
  });
  const clearAllFilters = () => {
    resetFilters();
    setTempRangeDateFilterValue({ startDate: "", endDate: "" });
  };

  const setRangeDateFilterValue = () => {
    setFilterValue("startDate", tempRangeDateFilterValue.startDate);
    setFilterValue("endDate", tempRangeDateFilterValue.endDate);
  };

  const updateTempRangeDateFilter = (value: [string, string]) => {
    setTempRangeDateFilterValue({ startDate: value[0], endDate: value[1] });
  };

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
              options={statusFilterOptions(filterValues)}
              selectedValue={filterValues.status}
              onSelect={(value) => setFilterValue("status", value)}
              isDisabled={!["sync", "clear", "refresh", ""].includes(filterValues.eventCategory)}
            />
          </FlexItem>
          <FlexItem>
            <ListBox
              buttonClassName={styles.filterButton}
              optionClassName={styles.filterOption}
              optionTextAs="span"
              options={eventTypeFilterOptions(filterValues)}
              selectedValue={filterValues.eventCategory}
              onSelect={(value) => setFilterValue("eventCategory", value)}
            />
          </FlexItem>
          <FlexItem>
            <RangeDatePicker
              value={[tempRangeDateFilterValue.startDate, tempRangeDateFilterValue.endDate]}
              onChange={updateTempRangeDateFilter}
              onClose={setRangeDateFilterValue}
              minDate={filterStart}
              maxDate={END_OF_TODAY}
              valueFormat="unix"
              buttonText="connection.timeline.rangeDateFilter"
            />
          </FlexItem>
        </>
      )}

      {!filtersAreDefault && (
        <FlexItem>
          <ClearFiltersButton onClick={clearAllFilters} />
        </FlexItem>
      )}
    </FlexContainer>
  );
};

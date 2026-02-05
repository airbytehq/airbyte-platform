import dayjs from "dayjs";
import { Suspense, useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { RangeDatePicker } from "components/ui/DatePicker";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ListBox } from "components/ui/ListBox";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { PageContainer } from "components/ui/PageContainer";
import { Text } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import { useListDataplaneGroups, useOrganizationWorkerUsage } from "core/api";

import styles from "./DataWorkerUsage.module.scss";
import { UsageByWorkspaceGraph } from "./UsageByWorkspaceGraph";
import { findFirstRegionWithUsage, getRegionOptions, sortByNameAlphabetically } from "./utils";

// Data retention period agreed at PAT session
// Backend retains data worker usage data for 90 days
const DATA_RETENTION_DAYS = 90;
const DATE_FORMAT = "YYYY-MM-DD";
const NOW = new Date();
const MIN_DATE = dayjs(NOW).subtract(DATA_RETENTION_DAYS, "day").startOf("day").format(DATE_FORMAT);

export const DataWorkerUsage: React.FC = () => {
  // RangeDatePicker fires onChange even if only one date is selected. We store that state here, and when both dates
  // have been selected, they get stored in selectedDateRange, which is what we use to fetch data.
  const [tempDateRange, setTempDateRange] = useState<[string, string]>([
    dayjs(NOW).subtract(7, "day").startOf("day").format(DATE_FORMAT),
    dayjs(NOW).endOf("day").format(DATE_FORMAT),
  ]);
  const [selectedDateRange, setSelectedDateRange] = useState<[string, string]>([
    dayjs(NOW).subtract(7, "day").startOf("day").format(DATE_FORMAT),
    dayjs(NOW).endOf("day").format(DATE_FORMAT),
  ]);
  const regions = useListDataplaneGroups();
  const [selectedRegion, setSelectedRegion] = useState<string | null>(null);
  const [startDate, setStartDate] = useState<string | null>(null);
  const { formatMessage } = useIntl();

  const allUsage = useOrganizationWorkerUsage({
    startDate: selectedDateRange[0],
    endDate: selectedDateRange[1],
  });

  // Sort regions alphabetically for consistent ordering
  const sortedRegions = useMemo(() => [...regions].sort(sortByNameAlphabetically), [regions]);

  const regionOptions = useMemo(() => getRegionOptions(sortedRegions), [sortedRegions]);

  // Auto-select first region with usage data on mount
  useEffect(() => {
    // Only auto-select if no region is currently selected
    if (selectedRegion !== null || !sortedRegions.length) {
      return;
    }

    const regionWithUsage = findFirstRegionWithUsage(sortedRegions, allUsage);
    const bestRegionId = regionWithUsage?.dataplane_group_id ?? sortedRegions[0]?.dataplane_group_id ?? null;

    if (bestRegionId) {
      setSelectedRegion(bestRegionId);
    }
  }, [sortedRegions, allUsage, selectedRegion]);

  const computedMaxDate = useMemo(() => {
    if (!startDate) {
      // No start date selected yet - allow selecting any date up to today
      return dayjs(NOW).endOf("day").format(DATE_FORMAT);
    }

    const maxRangeDate = dayjs(startDate).add(30, "day");
    const today = dayjs(NOW);

    // MaxDate is the EARLIER of: (startDate + 30) or today
    // This ensures we never exceed today AND never exceed 30-day range
    return maxRangeDate.isBefore(today) ? maxRangeDate.format(DATE_FORMAT) : today.format(DATE_FORMAT);
  }, [startDate]);

  const handleDateChange = (dates: [string, string]) => {
    setTempDateRange([dates[0], dates[1]]);
    if (dates[0] !== "") {
      setStartDate(dates[0]);
    } else {
      setStartDate(null);
    }
  };

  const handleDatePickerClose = () => {
    if (tempDateRange[0] !== "" && tempDateRange[1] !== "") {
      setSelectedDateRange([tempDateRange[0], tempDateRange[1]]);
    } else {
      setTempDateRange(selectedDateRange);
    }
  };

  return (
    <Box mt="xl">
      <PageContainer>
        <FlexContainer direction="column" alignItems="stretch">
          <FlexContainer alignItems="center" justifyContent="space-between">
            <FlexItem>
              <ListBox
                options={regionOptions}
                onSelect={setSelectedRegion}
                selectedValue={selectedRegion}
                placeholder={formatMessage({ id: "settings.organization.usage.selectRegion" })}
              />
            </FlexItem>
            <FlexItem>
              <RangeDatePicker
                minDate={MIN_DATE}
                maxDate={computedMaxDate}
                value={tempDateRange}
                onChange={handleDateChange}
                onClose={handleDatePickerClose}
              />
            </FlexItem>
          </FlexContainer>
          <Box mt="xl">
            <FlexContainer direction="column" gap="lg">
              <FlexContainer alignItems="center" gap="sm">
                <Heading as="h2" size="sm">
                  <FormattedMessage id="settings.organization.usageByWorkspace" />
                </Heading>
                <InfoTooltip>
                  <FormattedMessage id="settings.organization.usageByWorkspace.tooltip" />
                </InfoTooltip>
              </FlexContainer>
              {selectedRegion && (
                <Suspense
                  fallback={
                    <FlexContainer
                      className={styles.dataWorkerUsage__loadingPlaceholder}
                      alignItems="center"
                      justifyContent="center"
                    >
                      <FlexContainer alignItems="center" gap="md">
                        <LoadingSpinner />
                        <Text>
                          <FormattedMessage id="settings.organization.usage.loadingUsageData" />
                        </Text>
                      </FlexContainer>
                    </FlexContainer>
                  }
                >
                  <UsageByWorkspaceGraph selectedRegionId={selectedRegion} dateRange={selectedDateRange} />
                </Suspense>
              )}
            </FlexContainer>
          </Box>
        </FlexContainer>
      </PageContainer>
    </Box>
  );
};

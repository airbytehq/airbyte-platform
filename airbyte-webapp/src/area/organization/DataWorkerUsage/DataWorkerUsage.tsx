import dayjs from "dayjs";
import { Suspense, useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { PageContainer } from "components/PageContainer";
import { Box } from "components/ui/Box";
import { RangeDatePicker } from "components/ui/DatePicker";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ListBox } from "components/ui/ListBox";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Text } from "components/ui/Text";

import { useListDataplaneGroups, useOrganizationWorkerUsage } from "core/api";

import styles from "./DataWorkerUsage.module.scss";
import { UsageByWorkspaceGraph } from "./UsageByWorkspaceGraph";
import { findFirstRegionWithUsage, getRegionOptions, sortByNameAlphabetically } from "./utils";

const NOW = new Date();

export const DataWorkerUsage: React.FC = () => {
  // RangeDatePicker fires onChange even if only one date is selected. We store that state here, and when both dates
  // have been selected, they get stored in selectedDateRange, which is what we use to fetch data.
  const [tempDateRange, setTempDateRange] = useState<[string, string]>([
    dayjs(NOW).subtract(30, "day").startOf("day").format("YYYY-MM-DD"),
    dayjs(NOW).endOf("day").format("YYYY-MM-DD"),
  ]);
  const [selectedDateRange, setSelectedDateRange] = useState<[string, string]>([
    dayjs(NOW).subtract(30, "day").startOf("day").format("YYYY-MM-DD"),
    dayjs(NOW).endOf("day").format("YYYY-MM-DD"),
  ]);
  const regions = useListDataplaneGroups();
  const [selectedRegion, setSelectedRegion] = useState<string | null>(null);
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
                maxDate={new Date().toString()}
                value={tempDateRange}
                onChange={(dates) => setTempDateRange([dates[0], dates[1]])}
                onClose={() => {
                  if (tempDateRange[0] !== "" && tempDateRange[1] !== "") {
                    setSelectedDateRange([tempDateRange[0], tempDateRange[1]]);
                  } else {
                    setTempDateRange(selectedDateRange);
                  }
                }}
              />
            </FlexItem>
          </FlexContainer>
          <Box mt="xl">
            <FlexContainer direction="column" gap="lg">
              <Heading as="h2" size="sm">
                <FormattedMessage id="settings.organization.usageByWorkspace" />
              </Heading>
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

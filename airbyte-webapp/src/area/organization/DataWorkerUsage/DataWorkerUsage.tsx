import classNames from "classnames";
import dayjs from "dayjs";
import { Suspense, useDeferredValue, useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ListBox, ListBoxControlButtonProps } from "components/ui/ListBox";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { useListDataplaneGroups, useOrganizationWorkerUsage } from "core/api";
import { useExperiment } from "core/services/Experiment";
import { useCurrentTime } from "core/utils/time";

import styles from "./DataWorkerUsage.module.scss";
import { RegionCapacityPanel } from "./RegionCapacityPanel";
import { UsageByWorkspaceGraph, UsageTimeRange } from "./UsageByWorkspaceGraph";
import { findFirstRegionWithUsage, getRegionOptions, sortByNameAlphabetically } from "./utils";

const TIME_RANGE_OPTIONS: Array<{ labelId: string; value: UsageTimeRange }> = [
  { labelId: "settings.organization.usage.timeRange.1d", value: "1d" },
  { labelId: "settings.organization.usage.timeRange.1w", value: "1w" },
  { labelId: "settings.organization.usage.timeRange.1m", value: "1m" },
  { labelId: "settings.organization.usage.timeRange.1q", value: "1q" },
  { labelId: "settings.organization.usage.timeRange.1y", value: "1y" },
];

const USAGE_UPDATE_INTERVAL_BY_RANGE: Record<UsageTimeRange, number> = {
  "1d": 60_000,
  "1w": 60_000,
  "1m": 60_000,
  "1q": 300_000,
  "1y": 300_000,
};

const RegionControlButtonContent = ({
  selectedOption,
  isDisabled,
  placeholder,
}: ListBoxControlButtonProps<string | null>) =>
  selectedOption ? (
    <FlexContainer as="span" alignItems="center" gap="sm">
      <Icon type="globe" size="sm" color={isDisabled ? "disabled" : undefined} />
      <Text as="span" size="lg" color={isDisabled ? "grey300" : "darkBlue"}>
        {selectedOption.label}
      </Text>
    </FlexContainer>
  ) : (
    <Text as="span" size="lg" color="grey">
      {placeholder}
    </Text>
  );

export const DataWorkerUsage: React.FC = () => {
  const [selectedTimeRange, setSelectedTimeRange] = useState<UsageTimeRange>("1w");
  const [comparisonEnabled, setComparisonEnabled] = useState(false);
  // Gating the render also gates the fetch: the allocation list is requested inside the panel, so an
  // organization without the flag never calls the admin-only /data_worker_allocation/list endpoint.
  const showRegionCapacity = useExperiment("platform.enable-data-worker-allocation");
  const regions = useListDataplaneGroups();
  const [selectedRegion, setSelectedRegion] = useState<string | null>(null);
  const { formatMessage } = useIntl();
  const timeRangeOptions = useMemo(
    () => TIME_RANGE_OPTIONS.map((option) => ({ ...option, label: formatMessage({ id: option.labelId }) })),
    [formatMessage]
  );

  const currentTime = useCurrentTime(USAGE_UPDATE_INTERVAL_BY_RANGE[selectedTimeRange]);

  const timeWindow = useMemo(() => {
    const boundaryGranularity = selectedTimeRange === "1d" || selectedTimeRange === "1w" ? "hour" : "day";
    const rangeEnd = dayjs(currentTime).startOf(boundaryGranularity).add(1, boundaryGranularity);
    const rangeStart =
      selectedTimeRange === "1d"
        ? rangeEnd.subtract(24, "hour")
        : selectedTimeRange === "1w"
        ? rangeEnd.subtract(7 * 24, "hour")
        : selectedTimeRange === "1m"
        ? rangeEnd.subtract(1, "month")
        : selectedTimeRange === "1q"
        ? rangeEnd.subtract(3, "month")
        : rangeEnd.subtract(1, "year");
    const displayRange: [string, string] = [rangeStart.toISOString(), rangeEnd.toISOString()];
    const requestDateRange: [string, string] = [
      rangeStart.toISOString().slice(0, 10),
      rangeEnd.subtract(1, "millisecond").toISOString().slice(0, 10),
    ];
    const historicalRangeEnd = rangeStart;
    const historicalRangeStart =
      selectedTimeRange === "1d"
        ? historicalRangeEnd.subtract(24, "hour")
        : selectedTimeRange === "1w"
        ? historicalRangeEnd.subtract(7 * 24, "hour")
        : selectedTimeRange === "1m"
        ? historicalRangeEnd.subtract(1, "month")
        : selectedTimeRange === "1q"
        ? historicalRangeEnd.subtract(3, "month")
        : historicalRangeEnd.subtract(1, "year");
    const historicalDisplayRange: [string, string] = [
      historicalRangeStart.toISOString(),
      historicalRangeEnd.toISOString(),
    ];
    const historicalRequestDateRange: [string, string] = [
      historicalRangeStart.toISOString().slice(0, 10),
      historicalRangeEnd.subtract(1, "millisecond").toISOString().slice(0, 10),
    ];

    return {
      displayRange,
      requestDateRange,
      historicalDisplayRange,
      historicalRequestDateRange,
      timeRange: selectedTimeRange,
    };
  }, [selectedTimeRange, currentTime]);
  // The clock tick re-anchors the window with a plain setState, outside any transition.
  // Deferring the derived window keeps the mounted chart visible while the re-keyed
  // usage query suspends (at UTC midnight the request dates change and there is no cache).
  const {
    displayRange,
    requestDateRange,
    historicalDisplayRange,
    historicalRequestDateRange,
    timeRange: displayedTimeRange,
  } = useDeferredValue(timeWindow);
  const usageUpdateInterval = USAGE_UPDATE_INTERVAL_BY_RANGE[displayedTimeRange];
  const isLoadingTimeRange = selectedTimeRange !== displayedTimeRange;

  const allUsage = useOrganizationWorkerUsage(
    {
      startDate: requestDateRange[0],
      endDate: requestDateRange[1],
    },
    usageUpdateInterval
  );

  const sortedRegions = useMemo(() => [...regions].sort(sortByNameAlphabetically), [regions]);
  const regionOptions = useMemo(() => getRegionOptions(sortedRegions), [sortedRegions]);

  useEffect(() => {
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
      <FlexContainer direction="column" alignItems="stretch" gap="2xl">
        <FlexContainer direction="column" alignItems="stretch" gap="lg">
          <FlexContainer direction="column" alignItems="stretch" gap="sm">
            <Heading as="h2" size="sm">
              <FormattedMessage id="settings.organization.usageByWorkspace" />
            </Heading>
            <Text color="grey" size="lg">
              <FormattedMessage id="settings.organization.usageByWorkspace.description" />
            </Text>
          </FlexContainer>
          <FlexContainer alignItems="center" gap="md">
            <fieldset className={styles.dataWorkerUsage__timeRangeControl}>
              <legend className={styles.dataWorkerUsage__timeRangeLegend}>
                <FormattedMessage id="settings.organization.usage.timeRange.legend" />
              </legend>
              {timeRangeOptions.map((option) => (
                <label
                  key={option.value}
                  htmlFor={`organization-data-worker-usage-time-range-${option.value}`}
                  className={classNames(styles.dataWorkerUsage__timeRangeOption, {
                    [styles["dataWorkerUsage__timeRangeOption--selected"]]: option.value === selectedTimeRange,
                  })}
                >
                  <input
                    id={`organization-data-worker-usage-time-range-${option.value}`}
                    type="radio"
                    name="organization-data-worker-usage-time-range"
                    value={option.value}
                    checked={selectedTimeRange === option.value}
                    onChange={() => setSelectedTimeRange(option.value)}
                    className={styles.dataWorkerUsage__timeRangeInput}
                  />
                  <Text
                    color={option.value === selectedTimeRange ? "darkBlue" : "grey"}
                    className={styles.dataWorkerUsage__timeRangeLabel}
                    as="span"
                    size="lg"
                    bold
                  >
                    {option.label}
                  </Text>
                </label>
              ))}
            </fieldset>
            <FlexItem>
              <ListBox
                options={regionOptions}
                onSelect={setSelectedRegion}
                selectedValue={selectedRegion}
                placeholder={formatMessage({ id: "settings.organization.usage.selectRegion" })}
                controlButtonContent={RegionControlButtonContent}
              />
            </FlexItem>
            <FlexItem grow />
            <FlexContainer alignItems="center" gap="sm">
              {/* eslint-disable-next-line jsx-a11y/label-has-associated-control -- the input is rendered by Switch */}
              <label htmlFor="organization-data-worker-usage-comparison">
                <Text as="span" size="lg">
                  <FormattedMessage id="dataWorkerUsage.comparison.toggle" />
                </Text>
              </label>
              <Switch
                id="organization-data-worker-usage-comparison"
                checked={comparisonEnabled}
                onChange={(event) => setComparisonEnabled(event.target.checked)}
                size="sm"
              />
            </FlexContainer>
          </FlexContainer>
          {selectedRegion && isLoadingTimeRange ? (
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
          ) : selectedRegion ? (
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
              <UsageByWorkspaceGraph
                selectedRegionId={selectedRegion}
                requestDateRange={requestDateRange}
                displayRange={displayRange}
                historicalRequestDateRange={historicalRequestDateRange}
                historicalDisplayRange={historicalDisplayRange}
                selectedTimeRange={displayedTimeRange}
                comparisonEnabled={comparisonEnabled}
                committedDataWorkers={allUsage?.committedDataWorkers}
              />
            </Suspense>
          ) : null}
        </FlexContainer>
        {showRegionCapacity && (
          <Suspense
            fallback={
              <FlexContainer
                className={styles.dataWorkerUsage__loadingPlaceholder}
                alignItems="center"
                justifyContent="center"
              >
                <LoadingSpinner />
              </FlexContainer>
            }
          >
            <RegionCapacityPanel
              regions={sortedRegions}
              selectedRegionId={selectedRegion}
              onSelectRegion={setSelectedRegion}
              requestDateRange={requestDateRange}
              displayRange={displayRange}
              selectedTimeRange={displayedTimeRange}
            />
          </Suspense>
        )}
      </FlexContainer>
    </Box>
  );
};

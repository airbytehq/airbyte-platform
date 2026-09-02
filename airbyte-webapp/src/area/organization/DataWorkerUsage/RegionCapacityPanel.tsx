import classNames from "classnames";
import { useCallback, useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptions, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { useListDataWorkerAllocations, useOrganizationWorkerUsage, useReallocateDataWorkerCapacity } from "core/api";
import { DataplaneGroupRead } from "core/api/types/AirbyteClient";

import { calculateGraphData, UsageGraphGranularity } from "./calculateGraphData";
import styles from "./RegionCapacityPanel.module.scss";
import { UsageTimeRange } from "./UsageByWorkspaceGraph";

/**
 * Capacity is contracted as a fixed total, so the API only ever reallocates it between two regions
 * rather than creating or destroying it. Each press of a stepper therefore reallocates exactly one
 * Data Worker and has to name the region on the other side of the reallocation.
 */
const REALLOCATE_AMOUNT = 1;

/**
 * Presses are staged locally and sent as one call per region pair once they stop, so reallocating
 * several Data Workers costs one request per pair instead of one per press.
 */
const REALLOCATE_DEBOUNCE_MS = 1000;

type ReallocateDirection = "give" | "take";

const pairKey = (fromRegionId: string, toRegionId: string) => `${fromRegionId}|${toRegionId}`;

interface StepperProps {
  icon: "minus" | "plus";
  label: string;
  testId: string;
  disabled: boolean;
  options: DropdownMenuOptions;
  onSelect: (option: DropdownMenuOptionType) => void;
}

/**
 * One stepper control.
 */
const Stepper: React.FC<StepperProps> = ({ icon, label, testId, disabled, options, onSelect }) => {
  const button = (
    <Button
      variant="secondary"
      size="xs"
      iconSize="sm"
      className={styles.regionCapacityPanel__stepper}
      icon={icon}
      disabled={disabled}
      aria-label={label}
      data-testid={testId}
    />
  );

  if (disabled) {
    return button;
  }

  return (
    <DropdownMenu placement="bottom" options={options} onChange={onSelect}>
      {() => button}
    </DropdownMenu>
  );
};

interface RegionCapacityPanelProps {
  regions: DataplaneGroupRead[];
  selectedRegionId: string | null;
  onSelectRegion: (dataplaneGroupId: string) => void;
  requestDateRange: [string, string];
  displayRange: [string, string];
  selectedTimeRange: UsageTimeRange;
}

export const RegionCapacityPanel: React.FC<RegionCapacityPanelProps> = ({
  regions,
  selectedRegionId,
  onSelectRegion,
  requestDateRange,
  displayRange,
  selectedTimeRange,
}) => {
  const { formatMessage, formatNumber } = useIntl();
  const allocationList = useListDataWorkerAllocations();
  const { mutateAsync: reallocateCapacity } = useReallocateDataWorkerCapacity();
  const allUsage = useOrganizationWorkerUsage({
    startDate: requestDateRange[0],
    endDate: requestDateRange[1],
  });

  // Staged reallocations that have not been sent yet, keyed by source|destination.
  const [pendingReallocations, setPendingReallocations] = useState<Record<string, number>>({});
  const [isFlushing, setIsFlushing] = useState(false);

  // Capacity is contracted as a fixed total that is always fully allocated, so the sum the
  // allocation table records is the organization's contracted figure. Reallocating leaves it
  // untouched — only /add_capacity and /remove_capacity can update it.
  const contractedDataWorkers = allocationList?.total_allocated_capacity;

  // A region the organization holds nothing in is absent from the response rather than present with
  // a zero, so every region gets a row and missing ones fall back to zero.
  const allocatedByRegionId = useMemo(() => {
    const allocated: Record<string, number> = {};
    allocationList?.allocations.forEach((allocation) => {
      allocated[allocation.dataplane_group_id] = allocation.allocated_capacity;
    });
    return allocated;
  }, [allocationList]);

  const pendingDeltaByRegionId = useMemo(() => {
    const delta: Record<string, number> = {};
    Object.entries(pendingReallocations).forEach(([key, amount]) => {
      const [fromRegionId, toRegionId] = key.split("|");
      delta[fromRegionId] = (delta[fromRegionId] ?? 0) - amount;
      delta[toRegionId] = (delta[toRegionId] ?? 0) + amount;
    });
    return delta;
  }, [pendingReallocations]);

  // What a row shows: the server's number plus anything staged against it, so a press lands on
  // screen immediately even though the request is still a moment away.
  const allocationFor = useCallback(
    (regionId: string) => (allocatedByRegionId[regionId] ?? 0) + (pendingDeltaByRegionId[regionId] ?? 0),
    [allocatedByRegionId, pendingDeltaByRegionId]
  );

  // Bucket size the chart uses for the same range, so the rows read the same records the same way.
  const granularity: UsageGraphGranularity =
    selectedTimeRange === "1y" ? "week" : selectedTimeRange === "1m" || selectedTimeRange === "1q" ? "day" : "hour";

  // Peak concurrent usage, taken from the chart's own bucketing so a row reports the largest
  // "Region max" the tooltip would show for that region across the selected range.
  const peakByRegionId = useMemo(() => {
    const peaks: Record<string, number> = {};
    regions.forEach((region) => {
      const regionUsage = allUsage?.regions.find((usage) => usage.id === region.dataplane_group_id);
      peaks[region.dataplane_group_id] = calculateGraphData(displayRange, granularity, regionUsage).reduce(
        (peak, bucket) => Math.max(peak, bucket.regionUsage),
        0
      );
    });
    return peaks;
  }, [regions, allUsage, displayRange, granularity]);

  useEffect(() => {
    if (isFlushing || Object.keys(pendingReallocations).length === 0) {
      return;
    }

    const timer = setTimeout(async () => {
      setIsFlushing(true);
      try {
        // The endpoint reallocates one pair per call, so a batch spanning several pairs sends one
        // call each, in sequence — a failure then stops the rest instead of racing them.
        for (const [key, amount] of Object.entries(pendingReallocations)) {
          const [fromRegionId, toRegionId] = key.split("|");
          await reallocateCapacity({ fromDataplaneGroupId: fromRegionId, toDataplaneGroupId: toRegionId, amount });
          // Dropping the settled pair in the same resolution as the hook's cache write keeps both
          // updates in one render, so the row never shows the staged delta on top of the new total.
          setPendingReallocations(({ [key]: _settled, ...rest }) => rest);
        }
      } catch {
        // The hook reports the failure and refetches the truth; discarding the rest of the batch
        // drops the rows back to what the server actually holds.
        setPendingReallocations({});
      } finally {
        setIsFlushing(false);
      }
    }, REALLOCATE_DEBOUNCE_MS);

    return () => clearTimeout(timer);
  }, [pendingReallocations, isFlushing, reallocateCapacity]);

  const buildReallocateOptions = (regionId: string, direction: ReallocateDirection): DropdownMenuOptions => [
    {
      as: "div",
      children: (
        <Box px="md" pt="sm" pb="xs">
          <Text size="sm" color="grey">
            <FormattedMessage
              id={
                direction === "give"
                  ? "settings.organization.usage.capacity.reallocateTo"
                  : "settings.organization.usage.capacity.reallocateFrom"
              }
              values={{ amount: REALLOCATE_AMOUNT }}
            />
          </Text>
        </Box>
      ),
    },
    ...regions
      .filter((region) => region.dataplane_group_id !== regionId)
      .map((region) => {
        const capacity = allocationFor(region.dataplane_group_id);
        return {
          displayName: formatMessage(
            { id: "settings.organization.usage.capacity.regionOption" },
            { name: region.name, capacity: formatNumber(capacity) }
          ),
          value: region.dataplane_group_id,
          // Nothing to take from a region that holds less than a whole Data Worker.
          disabled: direction === "take" && capacity < REALLOCATE_AMOUNT,
        };
      }),
  ];

  const handleReallocate = (regionId: string, direction: ReallocateDirection) => (option: DropdownMenuOptionType) => {
    const otherRegionId = option.value as string;
    const fromRegionId = direction === "give" ? regionId : otherRegionId;
    const toRegionId = direction === "give" ? otherRegionId : regionId;

    // The source has to hold what is being taken out of it. The stepper is already hidden behind a
    // disabled button in that case, but this keeps a stale menu — one opened before another press
    // drained the source — from staging a move that would render a negative row.
    if (allocationFor(fromRegionId) < REALLOCATE_AMOUNT) {
      return;
    }

    setPendingReallocations((pending) => {
      const reverseKey = pairKey(toRegionId, fromRegionId);
      // A press that undoes a staged one cancels it instead of queueing an opposing call.
      if (pending[reverseKey]) {
        const remaining = pending[reverseKey] - REALLOCATE_AMOUNT;
        const { [reverseKey]: _cancelled, ...rest } = pending;
        return remaining > 0 ? { ...rest, [reverseKey]: remaining } : rest;
      }

      const forwardKey = pairKey(fromRegionId, toRegionId);
      return { ...pending, [forwardKey]: (pending[forwardKey] ?? 0) + REALLOCATE_AMOUNT };
    });
  };

  return (
    <FlexContainer direction="column" gap="sm">
      <FlexContainer alignItems="baseline" justifyContent="space-between">
        <Heading as="h2" size="sm">
          <FormattedMessage id="settings.organization.usage.capacity.title" />
        </Heading>
        {contractedDataWorkers != null && (
          <Text size="lg" color="grey">
            <FormattedMessage
              id="settings.organization.usage.capacity.contracted"
              values={{
                count: formatNumber(contractedDataWorkers),
                strong: (node: React.ReactNode) => (
                  <Text as="span" size="lg">
                    {node}
                  </Text>
                ),
              }}
            />
          </Text>
        )}
      </FlexContainer>
      <Text color="grey" size="lg">
        <FormattedMessage id="settings.organization.usage.capacity.description" />
      </Text>

      <Box mt="md">
        <div className={styles.regionCapacityPanel__rows}>
          {regions.map((region) => {
            const regionId = region.dataplane_group_id;
            const allocated = allocationFor(regionId);
            const peak = peakByRegionId[regionId] ?? 0;
            const isSelected = regionId === selectedRegionId;
            // Capacity can only come from a region that has some, so a lone funded region cannot take.
            const canTake = regions.some(
              (other) =>
                other.dataplane_group_id !== regionId && allocationFor(other.dataplane_group_id) >= REALLOCATE_AMOUNT
            );

            return (
              <div
                key={regionId}
                role="button"
                tabIndex={0}
                data-testid={`region-capacity-row-${regionId}`}
                onClick={() => onSelectRegion(regionId)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    onSelectRegion(regionId);
                  }
                }}
                className={classNames(styles.regionCapacityPanel__row, {
                  [styles["regionCapacityPanel__row--selected"]]: isSelected,
                })}
              >
                <FlexContainer alignItems="center" gap="sm" className={styles.regionCapacityPanel__name}>
                  <span
                    className={classNames(styles.regionCapacityPanel__marker, {
                      [styles["regionCapacityPanel__marker--selected"]]: isSelected,
                    })}
                  />
                  <Text size="sm">{region.name}</Text>
                </FlexContainer>

                {/* The steppers open their own menus, so a click here must not also reselect the row. */}
                <FlexContainer
                  alignItems="center"
                  gap="md"
                  onClick={(event) => event.stopPropagation()}
                  onKeyDown={(event) => event.stopPropagation()}
                >
                  {/* The stepper controls read as one unit, so they stay tight while the unit label
                      sits at the outer gap away from the increment button. */}
                  <FlexContainer alignItems="center" gap="xs">
                    <Stepper
                      icon="minus"
                      disabled={allocated < REALLOCATE_AMOUNT || isFlushing}
                      options={buildReallocateOptions(regionId, "give")}
                      onSelect={handleReallocate(regionId, "give")}
                      label={formatMessage(
                        { id: "settings.organization.usage.capacity.reallocateOut" },
                        { name: region.name }
                      )}
                      testId={`region-capacity-decrement-${regionId}`}
                    />
                    <Text size="sm" className={styles.regionCapacityPanel__value}>
                      {formatNumber(allocated)}
                    </Text>
                    <Stepper
                      icon="plus"
                      disabled={!canTake || isFlushing}
                      options={buildReallocateOptions(regionId, "take")}
                      onSelect={handleReallocate(regionId, "take")}
                      label={formatMessage(
                        { id: "settings.organization.usage.capacity.reallocateIn" },
                        { name: region.name }
                      )}
                      testId={`region-capacity-increment-${regionId}`}
                    />
                  </FlexContainer>
                  <Text size="sm" color="grey">
                    <FormattedMessage id="settings.organization.usage.capacity.unit" />
                  </Text>
                </FlexContainer>

                <div className={styles.regionCapacityPanel__meter}>
                  <div
                    className={styles.regionCapacityPanel__meterFill}
                    // Derived from live usage, so it cannot be expressed as a build-time class.
                    style={{ width: `${allocated > 0 ? Math.min(100, (peak / allocated) * 100) : 0}%` }}
                  />
                </div>

                <Text size="sm" color="grey" className={styles.regionCapacityPanel__peak}>
                  <FormattedMessage
                    id="settings.organization.usage.capacity.peak"
                    values={{
                      peak: formatNumber(peak, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
                      allocated: formatNumber(allocated),
                      strong: (node: React.ReactNode) => (
                        <Text as="span" size="sm">
                          {node}
                        </Text>
                      ),
                    }}
                  />
                </Text>
              </div>
            );
          })}
        </div>
      </Box>
    </FlexContainer>
  );
};

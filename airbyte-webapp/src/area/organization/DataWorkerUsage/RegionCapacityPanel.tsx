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
 * rather than creating or destroying it. A reallocation therefore always names the region on the
 * other side of it, and the smallest one anyone can make is half a Data Worker.
 */
const REALLOCATE_STEP = 0.5;

/** Amounts a stepper offers as one click, next to the amount control that walks by REALLOCATE_STEP. */
const AMOUNT_PRESETS = [0.5, 1, 2];

const DEFAULT_AMOUNT = 1;

/**
 * Data Workers move in halves, so a region's allocation carries the decimal that shows it. The
 * contracted total is not a per-region figure and stays whole, so it formats without this.
 */
const DW_FORMAT = { minimumFractionDigits: 1, maximumFractionDigits: 1 } as const;

/**
 * Presses are staged locally and sent as one call per region pair once they stop, so reallocating
 * several Data Workers costs one request per pair instead of one per press.
 */
const REALLOCATE_DEBOUNCE_MS = 1000;

type ReallocateDirection = "give" | "take";

/**
 * Either a fixed number of Data Workers or the source region's whole holding. "All" stays unresolved
 * until a region is picked because on a "take" the source — and so the amount — is the region clicked.
 */
type ReallocateAmount = number | "all";

const pairKey = (fromRegionId: string, toRegionId: string) => `${fromRegionId}|${toRegionId}`;

interface StepperProps {
  icon: "minus" | "plus";
  direction: ReallocateDirection;
  regionId: string;
  regionName: string;
  regions: DataplaneGroupRead[];
  allocationFor: (regionId: string) => number;
  disabled: boolean;
  testId: string;
  onReallocate: (otherRegionId: string, amount: number) => void;
}

/**
 * One stepper control and the menu it opens: how much to move, then the region to move it with.
 */
const Stepper: React.FC<StepperProps> = ({
  icon,
  direction,
  regionId,
  regionName,
  regions,
  allocationFor,
  disabled,
  testId,
  onReallocate,
}) => {
  const { formatMessage, formatNumber } = useIntl();
  const [amount, setAmount] = useState<ReallocateAmount>(DEFAULT_AMOUNT);

  const button = (
    <Button
      variant="secondary"
      size="xs"
      iconSize="sm"
      className={styles.regionCapacityPanel__stepper}
      icon={icon}
      disabled={disabled}
      aria-label={formatMessage(
        {
          id:
            direction === "give"
              ? "settings.organization.usage.capacity.reallocateOut"
              : "settings.organization.usage.capacity.reallocateIn",
        },
        { name: regionName }
      )}
      data-testid={testId}
    />
  );

  if (disabled) {
    return button;
  }

  const otherRegions = regions.filter((region) => region.dataplane_group_id !== regionId);

  // The endpoint moves capacity one pair at a time, so a single move can be no larger than what this
  // region holds when it is giving, or than the largest single holding elsewhere when it is taking.
  const maxAmount =
    direction === "give"
      ? allocationFor(regionId)
      : otherRegions.reduce((largest, region) => Math.max(largest, allocationFor(region.dataplane_group_id)), 0);

  // Another stepper can drain the source while this menu sits open, so the staged amount is clamped
  // on read rather than trusted from state.
  const selectedAmount: ReallocateAmount = amount === "all" ? "all" : Math.min(amount, maxAmount);
  const steppedAmount = selectedAmount === "all" ? maxAmount : selectedAmount;

  // The picker sits inside the menu panel, where Headless UI reserves Space and the arrow keys for
  // the region list below, so each of its controls keeps its own keys to itself.
  const keepKeysHere = (event: React.KeyboardEvent) => event.stopPropagation();

  const presetButton = (preset: ReallocateAmount) => (
    <button
      key={String(preset)}
      type="button"
      disabled={preset !== "all" && preset > maxAmount}
      onClick={() => setAmount(preset)}
      onKeyDown={keepKeysHere}
      data-testid={`${testId}-preset-${preset}`}
      className={classNames(styles.regionCapacityPanel__preset, {
        [styles["regionCapacityPanel__preset--selected"]]: selectedAmount === preset,
      })}
    >
      <Text size="sm" color={selectedAmount === preset ? "darkBlue" : "grey"}>
        {preset === "all" ? (
          <FormattedMessage id="settings.organization.usage.capacity.amountAll" />
        ) : (
          formatNumber(preset, DW_FORMAT)
        )}
      </Text>
    </button>
  );

  const options: DropdownMenuOptions = [
    {
      as: "div",
      className: styles.regionCapacityPanel__amountPicker,
      children: (
        <>
          <Box px="md" pt="sm">
            <Text size="sm" color="grey">
              <FormattedMessage
                id={
                  direction === "give"
                    ? "settings.organization.usage.capacity.moveTo"
                    : "settings.organization.usage.capacity.moveFrom"
                }
              />
            </Text>
          </Box>
          <Box px="md" py="sm">
            <FlexContainer alignItems="center" gap="lg">
              <FlexContainer alignItems="center" gap="xs">
                <Button
                  variant="secondary"
                  size="xs"
                  iconSize="sm"
                  icon="minus"
                  className={styles.regionCapacityPanel__stepper}
                  disabled={steppedAmount - REALLOCATE_STEP < REALLOCATE_STEP}
                  onClick={() => setAmount(Math.max(REALLOCATE_STEP, steppedAmount - REALLOCATE_STEP))}
                  onKeyDown={keepKeysHere}
                  aria-label={formatMessage({ id: "settings.organization.usage.capacity.decreaseAmount" })}
                  data-testid={`${testId}-amount-decrement`}
                />
                <Text size="sm" className={styles.regionCapacityPanel__amount}>
                  {selectedAmount === "all" ? (
                    <FormattedMessage id="settings.organization.usage.capacity.amountAll" />
                  ) : (
                    <FormattedMessage
                      id="settings.organization.usage.capacity.amountWithUnit"
                      values={{ amount: formatNumber(selectedAmount, DW_FORMAT) }}
                    />
                  )}
                </Text>
                <Button
                  variant="secondary"
                  size="xs"
                  iconSize="sm"
                  icon="plus"
                  className={styles.regionCapacityPanel__stepper}
                  disabled={selectedAmount === "all" || selectedAmount + REALLOCATE_STEP > maxAmount}
                  onClick={() => setAmount(Math.min(maxAmount, steppedAmount + REALLOCATE_STEP))}
                  onKeyDown={keepKeysHere}
                  aria-label={formatMessage({ id: "settings.organization.usage.capacity.increaseAmount" })}
                  data-testid={`${testId}-amount-increment`}
                />
              </FlexContainer>
              <FlexContainer alignItems="center" gap="xs">
                {AMOUNT_PRESETS.map(presetButton)}
                {presetButton("all")}
              </FlexContainer>
            </FlexContainer>
          </Box>
        </>
      ),
    },
    ...otherRegions.map((region) => {
      const capacity = allocationFor(region.dataplane_group_id);
      return {
        displayName: region.name,
        value: region.dataplane_group_id,
        iconPosition: "right" as const,
        icon: (
          <Text size="sm" color="grey">
            <FormattedMessage
              id="settings.organization.usage.capacity.amountWithUnit"
              values={{ amount: formatNumber(capacity, DW_FORMAT) }}
            />
          </Text>
        ),
        // Taking needs a source that holds the chosen amount; giving only needs somewhere to put it.
        disabled: direction === "take" && (selectedAmount === "all" ? capacity <= 0 : capacity < selectedAmount),
      };
    }),
  ];

  const handleSelect = (option: DropdownMenuOptionType) => {
    const otherRegionId = option.value as string;
    // "All" empties whichever region is the source of this move, which on a "take" is the one clicked.
    const sourceCapacity = direction === "give" ? allocationFor(regionId) : allocationFor(otherRegionId);
    onReallocate(otherRegionId, selectedAmount === "all" ? sourceCapacity : selectedAmount);
  };

  return (
    <DropdownMenu placement="bottom" options={options} onChange={handleSelect}>
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
  const { formatNumber } = useIntl();
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

  const handleReallocate =
    (regionId: string, direction: ReallocateDirection) => (otherRegionId: string, amount: number) => {
      const fromRegionId = direction === "give" ? regionId : otherRegionId;
      const toRegionId = direction === "give" ? otherRegionId : regionId;

      // The source has to hold what is being taken out of it. The menu already clamps the amount to
      // that, but this keeps a stale menu — one opened before another press drained the source — from
      // staging a move that would render a negative row.
      if (amount <= 0 || allocationFor(fromRegionId) < amount) {
        return;
      }

      setPendingReallocations((pending) => {
        const forwardKey = pairKey(fromRegionId, toRegionId);
        const reverseKey = pairKey(toRegionId, fromRegionId);
        const staged = pending[reverseKey] ?? 0;

        // A move back across a pair cancels what is staged the other way instead of queueing an
        // opposing call, and only what it cannot cancel is staged forward.
        if (staged > 0) {
          const { [reverseKey]: _cancelled, ...rest } = pending;
          if (staged > amount) {
            return { ...rest, [reverseKey]: staged - amount };
          }
          const overflow = amount - staged;
          return overflow > 0 ? { ...rest, [forwardKey]: (rest[forwardKey] ?? 0) + overflow } : rest;
        }

        return { ...pending, [forwardKey]: (pending[forwardKey] ?? 0) + amount };
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
                other.dataplane_group_id !== regionId && allocationFor(other.dataplane_group_id) >= REALLOCATE_STEP
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
                      direction="give"
                      regionId={regionId}
                      regionName={region.name}
                      regions={regions}
                      allocationFor={allocationFor}
                      disabled={allocated < REALLOCATE_STEP || isFlushing}
                      onReallocate={handleReallocate(regionId, "give")}
                      testId={`region-capacity-decrement-${regionId}`}
                    />
                    <Text size="sm" className={styles.regionCapacityPanel__value}>
                      {formatNumber(allocated, DW_FORMAT)}
                    </Text>
                    <Stepper
                      icon="plus"
                      direction="take"
                      regionId={regionId}
                      regionName={region.name}
                      regions={regions}
                      allocationFor={allocationFor}
                      disabled={!canTake || isFlushing}
                      onReallocate={handleReallocate(regionId, "take")}
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
                      allocated: formatNumber(allocated, DW_FORMAT),
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

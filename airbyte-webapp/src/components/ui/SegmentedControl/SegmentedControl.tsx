import { Radio, RadioGroup } from "@headlessui/react";
import classNames from "classnames";
import React from "react";

import styles from "./SegmentedControl.module.scss";

export interface SegmentedControlOption<T> {
  label: React.ReactNode;
  value: T;
  "data-testid"?: string;
}

type AccessibleName =
  | { "aria-label": string; "aria-labelledby"?: never }
  | { "aria-labelledby": string; "aria-label"?: never };

export type SegmentedControlProps<T> = AccessibleName & {
  options: Array<SegmentedControlOption<T>>;
  /** When undefined, no segment renders as selected. */
  selectedValue?: T;
  onSelect: (value: T) => void;
  isDisabled?: boolean;
  hasError?: boolean;
  className?: string;
  "data-testid"?: string;
};

/**
 * A single-choice control that shows every option side by side, with radiogroup semantics.
 * Arrow keys move the selection; Tab enters and leaves the group as one stop.
 * Segments share the available width equally and long labels wrap onto more lines.
 */
export const SegmentedControl = <T,>({
  options,
  selectedValue,
  onSelect,
  isDisabled,
  hasError,
  className,
  "aria-label": ariaLabel,
  "aria-labelledby": ariaLabelledBy,
  "data-testid": testId,
}: SegmentedControlProps<T>) => (
  <RadioGroup
    value={selectedValue ?? null}
    onChange={(value: T | null) => {
      if (value !== null) {
        onSelect(value);
      }
    }}
    disabled={isDisabled}
    aria-label={ariaLabel}
    aria-labelledby={ariaLabelledBy}
    aria-invalid={hasError || undefined}
    data-testid={testId}
    className={classNames(styles.segmentedControl, { [styles["segmentedControl--error"]]: hasError }, className)}
  >
    {options.map((option, index) => (
      <Radio
        key={index}
        value={option.value}
        data-testid={option["data-testid"]}
        className={styles.segmentedControl__segment}
      >
        {option.label}
      </Radio>
    ))}
  </RadioGroup>
);

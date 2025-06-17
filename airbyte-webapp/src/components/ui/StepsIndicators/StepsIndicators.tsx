import { Fragment } from "react";

import { FlexContainer } from "../Flex/FlexContainer";
import { Icon } from "../Icon";
import { NumberBadge } from "../NumberBadge";
import { Text } from "../Text/Text";

export enum StepStatus {
  COMPLETE = "complete",
  ACTIVE = "active",
  INCOMPLETE = "incomplete",
}

interface StepItemProps {
  state: StepStatus;
  label: React.ReactNode;
  value: number;
}

export const StepItem: React.FC<StepItemProps> = ({ state, label, value }) => {
  const color = state === StepStatus.INCOMPLETE ? "grey" : "blue";
  return (
    <FlexContainer alignItems="center" gap="sm">
      <NumberBadge value={value} outline={state !== StepStatus.ACTIVE} color={color} />
      <Text color={color} size="sm">
        {label}
      </Text>
    </FlexContainer>
  );
};

interface StepsIndicatorsProps {
  steps: Array<{ state: StepStatus; label: React.ReactNode }>;
}

export const StepsIndicators: React.FC<StepsIndicatorsProps> = ({ steps }) => (
  <FlexContainer gap="sm" alignItems="center">
    {steps.map((step, idx) => (
      <Fragment key={idx}>
        <StepItem state={step.state} label={step.label} value={idx + 1} />
        {idx !== steps.length - 1 && <Icon type="chevronRight" color="disabled" />}
      </Fragment>
    ))}
  </FlexContainer>
);

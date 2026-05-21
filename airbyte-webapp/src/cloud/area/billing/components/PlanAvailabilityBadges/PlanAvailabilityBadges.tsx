import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";

export type PlanAvailability = "plus" | "pro";

interface PlanAvailabilityBadgesProps {
  plans: PlanAvailability[];
}

const variantByPlan: Record<PlanAvailability, "green" | "blue"> = {
  plus: "green",
  pro: "blue",
};

export const PlanAvailabilityBadges: React.FC<PlanAvailabilityBadgesProps> = ({ plans }) => (
  <FlexContainer as="span" alignItems="center" gap="xs">
    {plans.map((plan) => (
      <Badge key={plan} variant={variantByPlan[plan]} uppercase={false}>
        <FormattedMessage id={`plans.${plan}.title`} />
      </Badge>
    ))}
  </FlexContainer>
);

import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

const CONSEQUENCE_MESSAGE_IDS = [
  "plans.standard.downgrade.consequence.syncFrequency",
  "plans.standard.downgrade.consequence.mappings",
  "plans.standard.downgrade.consequence.sso",
  "plans.standard.downgrade.consequence.workspaces",
  "plans.standard.downgrade.consequence.support",
];

/** Body of the "Downgrade to Standard" confirmation: what the org loses, and when the change takes effect. */
export const StandardDowngradeConsequences: React.FC = () => (
  <FlexContainer direction="column" gap="md">
    <Text>
      <FormattedMessage id="plans.standard.downgrade.confirmText" />
    </Text>
    <ul data-testid="downgrade-consequences">
      {CONSEQUENCE_MESSAGE_IDS.map((id) => (
        <li key={id}>
          <Text>
            <FormattedMessage id={id} />
          </Text>
        </li>
      ))}
    </ul>
    <Text>
      <FormattedMessage id="plans.standard.downgrade.timing" />
    </Text>
  </FlexContainer>
);

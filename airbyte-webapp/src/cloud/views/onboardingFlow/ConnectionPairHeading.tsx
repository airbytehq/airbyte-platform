import React from "react";
import { useIntl } from "react-intl";

import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";

import { ConnectorIcon } from "area/connector/components/ConnectorIcon";

import styles from "./ConnectionPairHeading.module.scss";
import { OnboardingPair } from "./useOnboardingPair";

type HeadingSize = "sm" | "lg";

const ARROW_SIZE: Record<HeadingSize, "md" | "lg"> = { sm: "md", lg: "lg" };

const Connector: React.FC<{ icon?: string; name: string }> = ({ icon, name }) => (
  <span className={styles.connector}>
    <ConnectorIcon icon={icon} />
    {name}
  </span>
);

interface ConnectionPairHeadingProps {
  pair: OnboardingPair;
  as: "h1" | "h2";
  size: HeadingSize;
}

/** A long connector name wraps to the next line as a whole, with its icon. The arrow reads as "to". */
export const ConnectionPairHeading: React.FC<ConnectionPairHeadingProps> = ({ pair, as, size }) => {
  const { formatMessage } = useIntl();

  return (
    <Heading as={as} size={size} className={styles.pair}>
      <Connector icon={pair.source.icon} name={pair.source.sourceName} />
      <Icon
        type="arrowRight"
        size={ARROW_SIZE[size]}
        title={formatMessage({ id: "onboardingFlow.connectionPair.direction" })}
      />
      <Connector icon={pair.destination.icon} name={pair.destination.destinationName} />
    </Heading>
  );
};

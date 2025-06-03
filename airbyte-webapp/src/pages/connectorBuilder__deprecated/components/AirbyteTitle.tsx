import { ReactNode } from "react";
import { FormattedMessage } from "react-intl";

import AirbyteLogo from "components/illustrations/airbyte-logo.svg?react";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import styles from "./AirbyteTitle.module.scss";

export const AirbyteTitle = ({ title }: { title: ReactNode }) => {
  return (
    <FlexContainer direction="column" gap="2xl">
      <FlexContainer direction="column" gap="md" alignItems="center" className={styles.titleContainer}>
        <AirbyteLogo />
        <Heading as="h1" size="lg" className={styles.title}>
          <FormattedMessage id="connectorBuilder.title" />
        </Heading>
      </FlexContainer>

      <Heading as="h1" size="lg" className={styles.heading}>
        {title}
      </Heading>
    </FlexContainer>
  );
};

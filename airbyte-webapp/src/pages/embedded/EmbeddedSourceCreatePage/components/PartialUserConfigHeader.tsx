import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { SvgIcon } from "area/connector/utils/SvgIcon";

import styles from "./PartialUserConfigHeader.module.scss";

export const PartialUserConfigHeader: React.FC<{ icon: string; connectorName: string }> = ({ icon, connectorName }) => {
  return (
    <FlexContainer alignItems="center" gap="sm" justifyContent="center">
      <FlexContainer className={styles.iconContainer} aria-hidden="true" alignItems="center">
        <SvgIcon src={icon} />
      </FlexContainer>
      <Heading as="h1" size="sm">
        {connectorName}
      </Heading>
    </FlexContainer>
  );
};

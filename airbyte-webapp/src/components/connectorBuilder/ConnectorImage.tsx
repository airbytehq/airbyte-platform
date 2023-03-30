import { useIntl } from "react-intl";

import { BuilderLogo } from "components/connectorBuilder/BuilderLogo";
import { FlexContainer } from "components/ui/Flex";

import styles from "./ConnectorImage.module.scss";

export const ConnectorImage = () => {
  const { formatMessage } = useIntl();

  /* TODO: replace with uploaded img when that functionality is added */
  return (
    <FlexContainer className={styles.connectorImg} alignItems="center">
      <BuilderLogo title={formatMessage({ id: "connectorBuilder.connectorImgAlt" })} />
    </FlexContainer>
  );
};

import React from "react";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";

import styles from "./BaseClearView.module.scss";
import { Version } from "../Version";

export const BaseClearView: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { formatMessage } = useIntl();

  return (
    <FlexContainer direction="column" justifyContent="space-between" alignItems="center" className={styles.content}>
      <FlexContainer direction="column" alignItems="center" className={styles.mainInfo}>
        <Link to="..">
          <img className={styles.logoImg} src="/logo.png" alt={formatMessage({ id: "ui.goBack" })} />
        </Link>
        {children}
      </FlexContainer>
      <Version />
    </FlexContainer>
  );
};

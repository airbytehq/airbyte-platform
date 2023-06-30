import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./SyncCatalogEmpty.module.scss";

interface SyncCatalogEmptyProps {
  customText?: string;
}

export const SyncCatalogEmpty: React.FC<SyncCatalogEmptyProps> = ({ customText }) => (
  <FlexContainer alignItems="center" justifyContent="center" className={styles.container}>
    <Text color="grey300" size="xl">
      <FormattedMessage id={customText ? customText : "connection.catalogTree.noStreams"} />
    </Text>
  </FlexContainer>
);

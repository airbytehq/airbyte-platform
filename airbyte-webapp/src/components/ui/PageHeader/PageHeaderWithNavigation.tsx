import React, { PropsWithChildren } from "react";

import styles from "./PageHeaderWithNavigation.module.scss";
import { Box } from "../Box";
import { BreadcrumbsDataItem, Breadcrumbs } from "../Breadcrumbs";
import { FlexContainer } from "../Flex";

interface PageHeaderWithNavigationProps {
  breadcrumbsData: BreadcrumbsDataItem[];
}

export const PageHeaderWithNavigation: React.FC<PropsWithChildren<PageHeaderWithNavigationProps>> = ({
  breadcrumbsData,
  children,
}) => {
  return (
    <FlexContainer direction="column" gap="none" className={styles.container}>
      <Box py="lg" px="xl" className={styles.section}>
        <Breadcrumbs data={breadcrumbsData} />
      </Box>
      {children && (
        <Box pt="lg" px="xl" className={styles.section}>
          <FlexContainer direction="column" gap="lg">
            {children}
          </FlexContainer>
        </Box>
      )}
    </FlexContainer>
  );
};

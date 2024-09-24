import classNames from "classnames";
import React, { PropsWithChildren } from "react";

import styles from "./PageHeaderWithNavigation.module.scss";
import { Box } from "../Box";
import { BreadcrumbsDataItem, Breadcrumbs } from "../Breadcrumbs";
import { FlexContainer } from "../Flex";

interface PageHeaderWithNavigationProps {
  breadcrumbsData: BreadcrumbsDataItem[];
  className?: string;
}

export const PageHeaderWithNavigation: React.FC<PropsWithChildren<PageHeaderWithNavigationProps>> = ({
  breadcrumbsData,
  className,
  children,
}) => {
  return (
    <FlexContainer direction="column" gap="none" className={classNames(styles.container, className)}>
      <Box px="xl" className={classNames(styles.section, styles.breadcrumbs)}>
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

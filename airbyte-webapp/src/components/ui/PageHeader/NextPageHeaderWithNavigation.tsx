import styles from "./NextPageHeaderWithNavigation.module.scss";
import { Box } from "../Box";
import { NextBreadcrumbsDataItem, NextBreadcrumbs } from "../Breadcrumbs/NextBreadcrumbs";
import { FlexContainer } from "../Flex";

interface NextPageHeaderWithNavigationProps {
  breadcrumbsData: NextBreadcrumbsDataItem[];
}

export const NextPageHeaderWithNavigation: React.FC<React.PropsWithChildren<NextPageHeaderWithNavigationProps>> = ({
  breadcrumbsData,
  children,
}) => {
  return (
    <FlexContainer direction="column" gap="none" className={styles.container}>
      <Box py="lg" px="xl" className={styles.section}>
        <NextBreadcrumbs data={breadcrumbsData} />
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

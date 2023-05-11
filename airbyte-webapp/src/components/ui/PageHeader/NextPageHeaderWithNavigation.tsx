import styles from "./NextPageHeaderWithNavigation.module.scss";
import { Box } from "../Box";
import { NavigationDataItem, NextBreadcrumbs } from "../Breadcrumbs/NextBreadcrumbs";
import { FlexContainer } from "../Flex";

interface NextPageHeaderWithNavigationProps {
  breadCrumbsData: NavigationDataItem[];
}

export const NextPageHeaderWithNavigation: React.FC<React.PropsWithChildren<NextPageHeaderWithNavigationProps>> = ({
  breadCrumbsData,
  children,
}) => {
  return (
    <FlexContainer direction="column" gap="none" className={styles.container}>
      <Box py="lg" px="xl" className={styles.section}>
        <NextBreadcrumbs data={breadCrumbsData} />
      </Box>
      <Box pt="lg" px="xl" className={styles.section}>
        <FlexContainer direction="column" gap="lg">
          {children}
        </FlexContainer>
      </Box>
    </FlexContainer>
  );
};

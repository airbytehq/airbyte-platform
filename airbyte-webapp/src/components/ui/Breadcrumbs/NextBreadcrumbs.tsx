import React, { Fragment } from "react";

import styles from "./NextBreadcrumbs.module.scss";
import { FlexContainer, FlexItem } from "../Flex";
import { Link } from "../Link";
import { Text } from "../Text";

export interface NavigationDataItem {
  label: string;
  to?: string;
}

export interface NextBreadcrumbsProps {
  data: NavigationDataItem[];
}

export const NextBreadcrumbs: React.FC<NextBreadcrumbsProps> = ({ data }) => {
  return (
    <>
      {data.length && (
        <FlexContainer className={styles.container}>
          {data.map((item, index) => (
            <Fragment key={item.label}>
              <FlexItem>
                {item.to ? (
                  <Link to={item.to} className={styles.link}>
                    <Text size="sm" color={index === data.length - 1 ? "darkBlue" : "grey"}>
                      {item.label}
                    </Text>
                  </Link>
                ) : (
                  <Text size="sm">{item.label}</Text>
                )}
              </FlexItem>
              {index !== data.length - 1 && <Text size="sm"> / </Text>}
            </Fragment>
          ))}
        </FlexContainer>
      )}
    </>
  );
};

import React, { Fragment } from "react";

import styles from "./Breadcrumbs.module.scss";
import { FlexContainer, FlexItem } from "../Flex";
import { Link } from "../Link";
import { Text } from "../Text";

export interface BreadcrumbsDataItem {
  label: string;
  to?: string;
}

export interface BreadcrumbsProps {
  data: BreadcrumbsDataItem[];
}

export const Breadcrumbs: React.FC<BreadcrumbsProps> = ({ data }) => {
  return (
    <>
      {data.length && (
        <FlexContainer className={styles.container}>
          {data.map((item, index) => (
            <Fragment key={item.label}>
              <FlexItem>
                {item.to ? (
                  <Link to={item.to} className={styles.link}>
                    {item.label}
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

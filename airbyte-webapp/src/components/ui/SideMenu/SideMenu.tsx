import React from "react";

import { MenuItem } from "./MenuItem";
import styles from "./SideMenu.module.scss";
import { Box } from "../Box";
import { Text } from "../Text";
export interface SideMenuItem {
  path: string;
  name: string | React.ReactNode;
  indicatorCount?: number;
  component?: React.ComponentType;
  id?: string;
  /**
   * Will be called instead of the onSelect of the component if this link is clicked.
   */
  onClick?: () => void;
}

export interface CategoryItem {
  category?: string | React.ReactNode;
  routes: SideMenuItem[];
}

interface SideMenuProps {
  data: CategoryItem[];
  activeItem?: string;
  onSelect: (id: string) => void;
}

export const SideMenu: React.FC<SideMenuProps> = ({ data, onSelect, activeItem }) => {
  return (
    <nav className={styles.sideMenu__container}>
      {data.map((categoryItem, index) => (
        <Box pb="lg" key={index}>
          {categoryItem.category && (
            <Box pb="sm">
              <Text size="sm" bold color="grey" className={styles.sideMenu__categoryName}>
                {categoryItem.category}
              </Text>
            </Box>
          )}
          {categoryItem.routes.map((route) => (
            <MenuItem
              id={route.id}
              key={route.path}
              name={route.name}
              isActive={activeItem?.endsWith(route.path)}
              count={route.indicatorCount}
              onClick={route.onClick ?? (() => onSelect(route.path))}
            />
          ))}
        </Box>
      ))}
    </nav>
  );
};

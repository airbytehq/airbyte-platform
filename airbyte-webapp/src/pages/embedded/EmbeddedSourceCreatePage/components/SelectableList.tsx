import { ReactNode } from "react";

import { ListItemButton } from "./ListItemButton";
import styles from "./SelectableList.module.scss";

interface ListItem {
  id: string;
  name: string;
  icon?: string;
  configured?: boolean;
}

interface SelectableListProps<T extends ListItem> {
  items: T[];
  onSelect: (id: string) => void;
  emptyState?: ReactNode;
}

export const SelectableList = <T extends ListItem>({ items, onSelect, emptyState }: SelectableListProps<T>) => {
  if (items.length === 0 && emptyState) {
    return <>{emptyState}</>;
  }

  return (
    <ul className={styles.list}>
      {items.map((item) => (
        <li key={item.id}>
          <ListItemButton
            label={item.name}
            onClick={() => onSelect(item.id)}
            icon={item.icon}
            configured={item.configured}
          />
        </li>
      ))}
    </ul>
  );
};

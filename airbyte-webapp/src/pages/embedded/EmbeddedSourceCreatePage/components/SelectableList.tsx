import { ReactNode } from "react";

import { Box } from "components/ui/Box";
import { Heading } from "components/ui/Heading";

import { ListItemButton } from "./ListItemButton";
import styles from "./SelectableList.module.scss";

interface ListItem {
  id: string;
  name: string;
  icon?: string;
}

interface SelectableListProps<T extends ListItem> {
  items: T[];
  onSelect: (id: string) => void;
  title?: ReactNode;
  emptyState?: ReactNode;
}

export const SelectableList = <T extends ListItem>({ items, onSelect, title, emptyState }: SelectableListProps<T>) => {
  if (items.length === 0 && emptyState) {
    return <>{emptyState}</>;
  }

  return (
    <>
      {title && (
        <Box mb="md">
          <Heading size="md" as="h1">
            {title}
          </Heading>
        </Box>
      )}
      <ul className={styles.list}>
        {items.map((item) => (
          <li key={item.id}>
            <ListItemButton label={item.name} onClick={() => onSelect(item.id)} icon={item.icon} />
          </li>
        ))}
      </ul>
    </>
  );
};

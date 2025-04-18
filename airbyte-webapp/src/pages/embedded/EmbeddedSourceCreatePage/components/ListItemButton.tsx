import { ConnectorIcon } from "components/ConnectorIcon";
import { Box } from "components/ui/Box";

import styles from "./ListItemButton.module.scss";

interface ListItemButtonProps {
  label: string;
  onClick: () => void;
  icon?: string;
}

export const ListItemButton: React.FC<ListItemButtonProps> = ({ label, onClick, icon }) => {
  return (
    <Box py="sm">
      <button className={styles.button} onClick={onClick}>
        <ConnectorIcon icon={icon} />
        {label}
      </button>
    </Box>
  );
};

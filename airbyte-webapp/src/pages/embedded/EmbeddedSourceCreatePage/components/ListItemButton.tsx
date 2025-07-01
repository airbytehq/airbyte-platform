import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import { SvgIcon } from "area/connector/utils";

import styles from "./ListItemButton.module.scss";

interface ListItemButtonProps {
  label: string;
  onClick: () => void;
  icon?: string;
  configured?: boolean;
}

export const ListItemButton: React.FC<ListItemButtonProps> = ({ label, onClick, icon, configured }) => {
  return (
    <button className={styles.button} onClick={onClick} title={label}>
      {configured && (
        <div className={styles.configuredIcon}>
          <Icon type="successOutline" size="xl" color="primary" />
        </div>
      )}
      <div className={styles.buttonContent}>
        <FlexContainer className={styles.iconContainer} aria-hidden="true" alignItems="center">
          <SvgIcon src={icon} />
        </FlexContainer>
        <div className={styles.label}>{label}</div>
      </div>
    </button>
  );
};

import { FlexContainer } from "components/ui/Flex";

import { SvgIcon } from "area/connector/utils";

import styles from "./ListItemButton.module.scss";

interface ListItemButtonProps {
  label: string;
  onClick: () => void;
  icon?: string;
}

export const ListItemButton: React.FC<ListItemButtonProps> = ({ label, onClick, icon }) => {
  return (
    <button className={styles.button} onClick={onClick}>
      <div className={styles.buttonContent}>
        <FlexContainer className={styles.iconContainer} aria-hidden="true" alignItems="center">
          <SvgIcon src={icon} />
        </FlexContainer>
        <div className={styles.label}>{label}</div>
      </div>
    </button>
  );
};

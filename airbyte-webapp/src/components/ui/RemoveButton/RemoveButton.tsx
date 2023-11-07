import styles from "./RemoveButton.module.scss";
import { Icon } from "../Icon";

export const RemoveButton = ({ onClick }: { onClick: () => void }) => {
  return (
    <button type="button" className={styles.removeButton} onClick={onClick}>
      <Icon type="cross" />
    </button>
  );
};

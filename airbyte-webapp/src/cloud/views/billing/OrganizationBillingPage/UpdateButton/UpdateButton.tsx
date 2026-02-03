import { Icon } from "components/ui/Icon";

import styles from "./UpdateButton.module.scss";

interface UpdateButtonProps {
  onClick?: () => void;
  isLoading?: boolean;
}

export const UpdateButton: React.FC<React.PropsWithChildren<UpdateButtonProps>> = ({
  children,
  onClick,
  isLoading = false,
}) => {
  return (
    <button onClick={onClick} className={styles.updateButton}>
      {children}
      <Icon type={isLoading ? "loading" : "share"} size="xs" />
    </button>
  );
};

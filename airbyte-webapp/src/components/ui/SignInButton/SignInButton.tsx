import { PropsWithChildren } from "react";

import styles from "./SignInButton.module.scss";

interface SignInButtonProps {
  disabled?: boolean;
  onClick: () => void;
}

export const SignInButton: React.FC<PropsWithChildren<SignInButtonProps>> = ({
  children,
  disabled = false,
  onClick,
}) => {
  return (
    <button onClick={onClick} className={styles.signInButton} disabled={disabled}>
      {children}
    </button>
  );
};

import styles from "./InputContainer.module.scss";

export const InputContainer: React.FC<React.PropsWithChildren> = ({ children }) => {
  return <div className={styles.container}>{children}</div>;
};

import classNames from "classnames";

import styles from "./LoadingSpinner.module.scss";

interface SpinnerProps {
  className?: string;
}

export const LoadingSpinner: React.FC<SpinnerProps> = ({ className }) => (
  <svg width="22" height="22" viewBox="0 0 22 22" fill="none" className={classNames(styles.spinner, className)}>
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M11 20V22C17.0751 22 22 17.0751 22 11C22 4.92487 17.0751 0 11 0V2C15.9706 2 20 6.02944 20 11C20 15.9706 15.9706 20 11 20Z"
      fill="url(#paint0_linear_6258_114055)"
    />
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M0 11C0 4.92487 4.92487 0 11 0V2C6.02944 2 2 6.02944 2 11C2 15.9706 6.02944 20 11 20V22C4.92487 22 0 17.0751 0 11Z"
      fill="url(#paint1_linear_6258_114055)"
    />
    <defs>
      <linearGradient id="paint0_linear_6258_114055" x1="11" y1="2" x2="11" y2="20" gradientUnits="userSpaceOnUse">
        <stop className={styles.stop} stopOpacity="0.5" />
        <stop className={styles.stop} offset="1" />
      </linearGradient>
      <linearGradient id="paint1_linear_6258_114055" x1="11" y1="2" x2="11" y2="20" gradientUnits="userSpaceOnUse">
        <stop className={styles.stop} stopOpacity="0.5" />
        <stop className={styles.stop} offset="1" stopOpacity="0.1" />
      </linearGradient>
    </defs>
  </svg>
);

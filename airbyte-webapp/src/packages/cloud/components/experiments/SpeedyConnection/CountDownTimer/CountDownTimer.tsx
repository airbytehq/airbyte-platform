import { Text } from "components/ui/Text";

import styles from "./CountDownTimer.module.scss";
import { useCountdown } from "./useCountdown";
export const CountDownTimer: React.FC<{ expiredOfferDate: string }> = ({ expiredOfferDate }) => {
  const [hours, minutes] = useCountdown(expiredOfferDate);

  return (
    <Text as="span" bold className={styles.countDownTimer}>
      {hours.toString().padStart(2, "0")}h {minutes.toString().padStart(2, "0")}m
    </Text>
  );
};

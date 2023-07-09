import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./SpeedyConnectionBanner.module.scss";
import { CountDownTimer } from "../CountDownTimer";
import { useExperimentSpeedyConnection } from "../hooks/useExperimentSpeedyConnection";

export const SpeedyConnectionBanner = () => {
  const { expiredOfferDate } = useExperimentSpeedyConnection();

  return (
    <FlexContainer alignItems="center" className={styles.speedyConnectionbanner__container}>
      <Text className={styles.speedyConnectionbanner__message}>
        <FormattedMessage
          id="experiment.speedyConnection"
          values={{
            link: (link: React.ReactNode[]) => (
              <Link
                className={styles.speedyConnectionbanner__cta}
                to={`${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}`}
              >
                {link}
              </Link>
            ),
            timer: () => <CountDownTimer expiredOfferDate={expiredOfferDate} />,
          }}
        />
      </Text>
    </FlexContainer>
  );
};

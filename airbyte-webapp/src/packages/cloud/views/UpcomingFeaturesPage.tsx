import { useIntl } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";

import { useExperiment } from "hooks/services/Experiment";
import { links } from "utils/links";

import styles from "./UpcomingFeaturesPage.module.scss";
import { useCurrentUser } from "../services/auth/AuthService";
const UpcomingFeaturesPage = () => {
  const { formatMessage } = useIntl();
  const user = useCurrentUser();
  const url = useExperiment("upcomingFeaturesPage.url", "");
  return (
    <>
      <HeadTitle titles={[{ id: "upcomingFeatures.title" }]} />
      <div className={styles.container}>
        <iframe
          className={styles.iframe}
          src={`${url || links.upcomingFeaturesPage}?email=${user?.email}`}
          title={formatMessage({ id: "upcomingFeatures.title" })}
        />
      </div>
    </>
  );
};

export default UpcomingFeaturesPage;

import { useIntl } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";

import { useCurrentUser } from "core/services/auth";
import { links } from "core/utils/links";
import { useExperiment } from "hooks/services/Experiment";

import styles from "./UpcomingFeaturesPage.module.scss";
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

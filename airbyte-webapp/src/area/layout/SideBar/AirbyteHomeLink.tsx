import React from "react";
import { useIntl } from "react-intl";

import { BrandingBadge, useGetProductBranding } from "components/ui/BrandingBadge";
import AirbyteLogo from "components/ui/illustrations/airbyte-logo.svg?react";
import { Link } from "components/ui/Link";

import { RoutePaths } from "pages/routePaths";

import styles from "./AirbyteHomeLink.module.scss";

export const AirbyteHomeLink: React.FC = () => {
  const { formatMessage } = useIntl();
  const product = useGetProductBranding();

  return (
    <div className={styles.homeLink}>
      <Link
        to={RoutePaths.Connections}
        aria-label={formatMessage({ id: "sidebar.homepage" })}
        className={styles.homeLink__link}
      >
        <AirbyteLogo height={25} className={styles.homeLink__logo} />
      </Link>
      <BrandingBadge product={product} testId={`${product}-badge`} />
    </div>
  );
};

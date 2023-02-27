import React from "react";
import { FormattedMessage } from "react-intl";

import { Card } from "components/ui/Card";

import { UsagePerDayGraph } from "./UsagePerDayGraph";
// import {Mock}
const CreditsUsage: React.FC = () => {
  return (
    <>
      <Card title={<FormattedMessage id="credits.totalUsage" />} lightPadding>
        <UsagePerDayGraph />
      </Card>

      {/* <Card title={<FormattedMessage id="credits.usagePerConnection" />} lightPadding className={styles.cardBlock}>
        <UsagePerConnectionTable />
      </Card> */}
    </>
  );
};

export default CreditsUsage;

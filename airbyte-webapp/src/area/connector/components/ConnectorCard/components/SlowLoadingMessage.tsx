import React, { useEffect, useState } from "react";
import { FormattedMessage } from "react-intl";

import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useIsCloudApp } from "core/utils/app";
import { links } from "core/utils/links";

interface SlowLoadingMessageProps {
  connector?: string;
}

const TIMEOUT_MS = 10000;

const SlowLoadingMessage: React.FC<SlowLoadingMessageProps> = ({ connector }) => {
  const [longLoading, setLongLoading] = useState(false);
  const isCloudApp = useIsCloudApp();
  useEffect(() => {
    setLongLoading(false);
    const timer = setTimeout(() => setLongLoading(true), TIMEOUT_MS);
    return () => clearTimeout(timer);
  }, [connector]);

  return longLoading ? (
    <Text align="center">
      <FormattedMessage
        id="form.tooLong"
        values={{
          lnk: (...lnk: React.ReactNode[]) => (
            <ExternalLink href={isCloudApp ? links.supportPortal : links.technicalSupport}>{lnk}</ExternalLink>
          ),
        }}
      />
    </Text>
  ) : null;
};

export default SlowLoadingMessage;

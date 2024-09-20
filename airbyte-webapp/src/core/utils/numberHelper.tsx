import { FormatNumberOptions, FormattedMessage, useIntl } from "react-intl";

export const formatBytes = (bytes?: number) => {
  if (bytes && bytes < 0) {
    bytes = 0;
  }

  if (!bytes) {
    return <FormattedMessage id="sources.countBytes" values={{ count: bytes || 0 }} />;
  }

  const k = 1024;
  const dm = 2;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB"] as const;
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  const result = parseFloat((bytes / Math.pow(k, i)).toFixed(dm));

  return <FormattedMessage id={`sources.count${sizes[i]}`} values={{ count: result }} />;
};

export const useFormatCredits = () => {
  const { formatNumber, formatMessage } = useIntl();

  return {
    formatCredits: (credits: number) => {
      const options: FormatNumberOptions = {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
        roundingMode: "halfCeil",
      };

      if (credits > 0 && credits < 0.01) {
        return formatMessage(
          { id: "settings.billing.credits.minimumVisibleAmount" },
          { zeroPointZeroOne: formatNumber(0.01, options) }
        );
      }

      if (credits > -0.01 && credits < 0) {
        return formatNumber(0.0, options);
      }

      return formatNumber(credits, options);
    },
  };
};

import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { ExternalLink } from "components/ui/Link";

import { NormalizationType } from "area/connection/types";
import { links } from "core/utils/links";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { LabeledRadioButtonFormControl } from "./LabeledRadioButtonFormControl";

/**
 * react-hook-form field for normalization operation
 */
export const NormalizationFormField: React.FC = () => {
  const { formatMessage } = useIntl();
  const { mode } = useConnectionFormService();

  return (
    <Box mt="lg" mb="lg">
      <LabeledRadioButtonFormControl
        name="normalization"
        controlId="normalization.raw"
        label={formatMessage({ id: "form.rawData" })}
        value={NormalizationType.raw}
        disabled={mode === "readonly"}
      />
      <LabeledRadioButtonFormControl
        name="normalization"
        controlId="normalization.basic"
        label={formatMessage({ id: "form.basicNormalization" })}
        value={NormalizationType.basic}
        disabled={mode === "readonly"}
        message={
          mode !== "readonly" && (
            <FormattedMessage
              id="form.basicNormalization.message"
              values={{
                lnk: (lnk: React.ReactNode) => <ExternalLink href={links.normalizationLink}>{lnk}</ExternalLink>,
              }}
            />
          )
        }
      />
    </Box>
  );
};

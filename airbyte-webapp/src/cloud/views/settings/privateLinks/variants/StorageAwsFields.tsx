import React from "react";
import { useIntl } from "react-intl";
import { z } from "zod";

import { FormControl } from "components/ui/forms";

import { PrivateLinkVariant } from "./types";

// Keep in sync with server-side OpenAPI validation in
// oss/airbyte-api/server-api/src/main/openapi/config.yaml (StoragePrivateLinkServiceConfig).
const AWS_REGION_REGEX = /^[a-z]{2,3}-[a-z]+(-[a-z]+)?-\d+$/;
const AWS_BUCKET_REGEX = /^(?!.*\.\.)(?!\d+\.\d+\.\d+\.\d+$)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;

const schema = z.object({
  variantId: z.literal("storage.aws"),
  region: z
    .string()
    .trim()
    .nonempty("form.empty.error")
    .regex(AWS_REGION_REGEX, "settings.privateLinks.form.region.invalid"),
  bucket: z
    .string()
    .trim()
    .nonempty("form.empty.error")
    .min(3, "settings.privateLinks.form.bucket.invalid")
    .max(63, "settings.privateLinks.form.bucket.invalid")
    .regex(AWS_BUCKET_REGEX, "settings.privateLinks.form.bucket.invalid"),
});

const Fields: React.FC = () => {
  const { formatMessage } = useIntl();
  return (
    <>
      <FormControl
        name="region"
        fieldType="input"
        label={formatMessage({ id: "settings.privateLinks.form.region" })}
        placeholder={formatMessage({ id: "settings.privateLinks.form.region.placeholder" })}
      />
      <FormControl
        name="bucket"
        fieldType="input"
        label={formatMessage({ id: "settings.privateLinks.form.bucket" })}
        placeholder={formatMessage({ id: "settings.privateLinks.form.bucket.placeholder" })}
        labelTooltip={formatMessage({ id: "settings.privateLinks.form.bucket.tooltip" })}
      />
    </>
  );
};

export const storageAwsVariant: PrivateLinkVariant<typeof schema> = {
  variantId: "storage.aws",
  serviceType: "storage",
  provider: "aws",
  labelKey: "settings.privateLinks.form.serviceType.storage",
  schema,
  defaultValues: { variantId: "storage.aws", region: "", bucket: "" },
  Fields,
  toCreateRequest: (values) => ({
    name: values.privateLinkName,
    serviceConfig: {
      type: "storage",
      region: values.region,
      bucket: values.bucket,
    },
  }),
};

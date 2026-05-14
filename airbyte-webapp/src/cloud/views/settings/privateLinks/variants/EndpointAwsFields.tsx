import React from "react";
import { useIntl } from "react-intl";
import { z } from "zod";

import { FormControl } from "components/ui/forms";

import { PrivateLinkVariant } from "./types";

const AWS_SERVICE_NAME_REGEX = /^com\.amazonaws\.vpce\.([a-z0-9-]+)\.vpce-svc-[a-z0-9]+$/;

const schema = z.object({
  variantId: z.literal("endpoint.aws"),
  serviceName: z
    .string()
    .trim()
    .nonempty("form.empty.error")
    .regex(AWS_SERVICE_NAME_REGEX, "settings.privateLinks.form.serviceName.invalid"),
});

const Fields: React.FC = () => {
  const { formatMessage } = useIntl();
  return (
    <FormControl
      name="serviceName"
      fieldType="input"
      label={formatMessage({ id: "settings.privateLinks.form.serviceName" })}
      placeholder={formatMessage({ id: "settings.privateLinks.form.serviceName.placeholder" })}
    />
  );
};

export const endpointAwsVariant: PrivateLinkVariant<typeof schema> = {
  variantId: "endpoint.aws",
  serviceType: "endpoint",
  provider: "aws",
  labelKey: "settings.privateLinks.form.serviceType.endpoint",
  schema,
  defaultValues: { variantId: "endpoint.aws", serviceName: "" },
  Fields,
  toCreateRequest: (values) => {
    // Safe non-null assertion — zod has already validated the regex matches.
    const region = values.serviceName.match(AWS_SERVICE_NAME_REGEX)![1];
    return {
      name: values.privateLinkName,
      serviceConfig: {
        type: "endpoint",
        name: values.serviceName,
        region,
      },
    };
  },
};

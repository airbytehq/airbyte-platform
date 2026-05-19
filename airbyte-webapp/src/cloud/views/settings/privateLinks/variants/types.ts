import { ComponentType } from "react";
import { z } from "zod";

import { PrivateLinkCreateRequestBody } from "core/api/types/AirbyteClient";

export type ServiceType = "endpoint" | "storage";
export type Provider = "aws" | "azure" | "gcp";

const PRIVATE_LINK_NAME_REGEX = /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/;

export const BasePrivateLinkSchema = z.object({
  privateLinkName: z
    .string()
    .trim()
    .nonempty("form.empty.error")
    .regex(PRIVATE_LINK_NAME_REGEX, "settings.privateLinks.form.name.invalid"),
});

export type BasePrivateLinkValues = z.infer<typeof BasePrivateLinkSchema>;

export const BASE_DEFAULTS: BasePrivateLinkValues = {
  privateLinkName: "",
};

export type CreatePrivateLinkBody = Omit<PrivateLinkCreateRequestBody, "workspaceId">;

export interface PrivateLinkVariant<S extends z.ZodObject<z.ZodRawShape> = z.ZodObject<z.ZodRawShape>> {
  variantId: string;
  serviceType: ServiceType;
  provider: Provider;
  labelKey: string;
  schema: S;
  defaultValues: z.infer<S>;
  Fields: ComponentType;
  toCreateRequest: (values: BasePrivateLinkValues & z.infer<S>) => CreatePrivateLinkBody;
}

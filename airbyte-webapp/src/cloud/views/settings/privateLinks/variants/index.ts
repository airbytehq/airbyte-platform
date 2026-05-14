import { endpointAwsVariant } from "./EndpointAwsFields";
import { storageAwsVariant } from "./StorageAwsFields";
import { PrivateLinkVariant } from "./types";

export { endpointAwsVariant, storageAwsVariant };

// Order here drives the order in the variant dropdown and the default selection (first entry).
// To add a new (target_class, provider) combination, create a {Class}{Provider}Fields.tsx and
// append its variant export here.
export const ALL_VARIANTS = [endpointAwsVariant, storageAwsVariant] as unknown as readonly PrivateLinkVariant[];

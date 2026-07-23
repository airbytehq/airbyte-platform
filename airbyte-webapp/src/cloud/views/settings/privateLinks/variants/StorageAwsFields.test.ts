import { storageAwsVariant } from "./StorageAwsFields";

const schema = storageAwsVariant.schema;

const baseValues = { variantId: "storage.aws", region: "us-east-1" };

describe("storageAwsVariant.schema", () => {
  describe("bucket", () => {
    it.each([["my-bucket"], ["airbyte-pl-storage-test-dev"], ["a1b"], ["bucket.with.periods"], ["a".repeat(63)]])(
      "accepts valid bucket name %s",
      (bucket) => {
        expect(schema.safeParse({ ...baseValues, bucket }).success).toBe(true);
      }
    );

    it.each([
      ["my..bucket", "adjacent periods"],
      ["1.2.3.4", "IP-address format"],
      ["10.0.0.1", "IP-address format"],
      ["My-Bucket", "uppercase letters"],
      ["-bucket", "leading hyphen"],
      ["bucket-", "trailing hyphen"],
      [".bucket", "leading period"],
      ["bucket.", "trailing period"],
      ["my_bucket", "underscore"],
      ["ab", "shorter than 3 chars"],
      ["a".repeat(64), "longer than 63 chars"],
    ])("rejects invalid bucket name %s (%s)", (bucket) => {
      expect(schema.safeParse({ ...baseValues, bucket }).success).toBe(false);
    });
  });

  describe("region", () => {
    it.each([["us-east-1"], ["eu-west-3"], ["ap-southeast-4"], ["us-gov-east-1"]])(
      "accepts valid region %s",
      (region) => {
        expect(schema.safeParse({ ...baseValues, region, bucket: "my-bucket" }).success).toBe(true);
      }
    );

    it.each([["US-EAST-1"], ["us_east_1"], ["us-east"], ["region1"], [""]])("rejects invalid region %s", (region) => {
      expect(schema.safeParse({ ...baseValues, region, bucket: "my-bucket" }).success).toBe(false);
    });
  });
});

import merge from "lodash/merge";
import { InferType, ValidationError } from "yup";

import { dbtOperationReadOrCreateSchema } from "./schema";

describe("<TransformationForm /> - validationSchema", () => {
  const customTransformationFields: InferType<typeof dbtOperationReadOrCreateSchema> = {
    name: "test name",
    workspaceId: "test workspace id",
    operationId: undefined,
    operatorConfiguration: {
      operatorType: "dbt",
      dbt: {
        gitRepoUrl: "https://github.com/username/example.git",
        dockerImage: undefined,
        dbtArguments: undefined,
        gitRepoBranch: "",
      },
    },
  };

  it("should successfully validate the schema", async () => {
    await expect(dbtOperationReadOrCreateSchema.validate(customTransformationFields)).resolves.toBeTruthy();
  });

  it("should fail if 'name' is empty", async () => {
    await expect(async () => {
      await dbtOperationReadOrCreateSchema.validateAt("name", { ...customTransformationFields, name: "" });
    }).rejects.toThrow(ValidationError);
  });

  it("should fail if 'gitRepoUrl' is empty", async () => {
    await expect(async () => {
      await dbtOperationReadOrCreateSchema.validateAt(
        "operatorConfiguration.dbt.gitRepoUrl",
        merge(customTransformationFields, {
          operatorConfiguration: { dbt: { gitRepoUrl: "" } },
        })
      );
    }).rejects.toThrow(ValidationError);
  });

  it("should successfully validate 'gitRepoUrl'", async () => {
    await expect(
      dbtOperationReadOrCreateSchema.validateAt(
        "operatorConfiguration.dbt.gitRepoUrl",
        merge(customTransformationFields, {
          operatorConfiguration: { dbt: { gitRepoUrl: "https://github.com/username/example.git" } },
        })
      )
    ).resolves.toBeTruthy();

    await expect(
      dbtOperationReadOrCreateSchema.validateAt(
        "operatorConfiguration.dbt.gitRepoUrl",
        merge(customTransformationFields, {
          operatorConfiguration: {
            dbt: { gitRepoUrl: "https://OrgName@dev.azure.com/OrgName/RepoName/_git/RepoName" },
          },
        })
      )
    ).resolves.toBeTruthy();
  });
});

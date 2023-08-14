import { SchemaOf } from "yup";
import * as yup from "yup";

import { DbtOperationReadOrCreate } from "./types";

export const dbtOperationReadOrCreateSchema: SchemaOf<DbtOperationReadOrCreate> = yup.object().shape({
  workspaceId: yup.string().required("form.empty.error"),
  operationId: yup.string().optional(), // during creation, this is not required
  name: yup.string().required("form.empty.error"),
  operatorConfiguration: yup
    .object()
    .shape({
      operatorType: yup.mixed().oneOf(["dbt"]).default("dbt"),
      dbt: yup.object({
        gitRepoUrl: yup.string().trim().required("form.empty.error"),
        gitRepoBranch: yup.string().optional(),
        dockerImage: yup.string().optional(),
        dbtArguments: yup.string().optional(),
      }),
    })
    .required(),
});

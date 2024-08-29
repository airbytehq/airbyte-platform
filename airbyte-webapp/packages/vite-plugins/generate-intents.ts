import type { Plugin } from "vite";

import fs from "node:fs";
import path from "node:path";

import chalk from "chalk";
import yaml from "js-yaml";

import { PermissionType } from "../../src/core/api/types/AirbyteClient";

// This is the shape of the JSON we expect to parse from intents.yaml
export interface IntentsYaml {
  intents: Record<string, IntentsYamlSingleIntent>;
}

export interface IntentsYamlSingleIntent {
  name: string;
  description: string;
  roles: Array<
    | "ADMIN"
    | "ORGANIZATION_ADMIN"
    | "ORGANIZATION_EDITOR"
    | "ORGANIZATION_READER"
    | "ORGANIZATION_MEMBER"
    | "WORKSPACE_ADMIN"
    | "WORKSPACE_EDITOR"
    | "WORKSPACE_READER"
  >;
}

// This is the shape of the intents we want to consume in the webapp. The roles match what we receive from the Airbyte API.
export interface IntentDefinition {
  name: string;
  description: string;
  roles: PermissionType[];
}

/**
 * Generates a strongly-typed generated-intents.ts file based on the intents.yaml defined in airbyte-commons-auth
 */
export function generateIntents(): Plugin {
  return {
    name: "airbyte/intent-generation",
    buildStart() {
      process.stdout.write(`🤖 Generate ${chalk.cyan("intents.ts")} file... `);
      const { intents } = yaml.load(
        fs.readFileSync(
          path.resolve(__dirname, "../../../airbyte-commons-auth/src/main/resources/intents.yaml"),
          "utf-8"
        )
      ) as { intents: IntentsYaml };

      const patchedIntents = patchRoleStrings(intents);

      fs.writeFileSync(
        path.resolve(__dirname, "../../src/core/utils/rbac/generated-intents.ts"),
        `// eslint-disable\nexport const INTENTS = ${JSON.stringify(patchedIntents, null, 2)} as const;`,
        "utf-8"
      );

      console.info(chalk.green("done!"));
    },
  };
}

function patchRoleStrings(intents: IntentsYaml): Record<string, IntentDefinition> {
  const patchedIntents: Record<string, IntentDefinition> = {};
  for (const key of Object.keys(intents)) {
    const intent = intents[key] as IntentsYamlSingleIntent;
    const patchedIntent: IntentDefinition = { ...intent, roles: [] };
    patchedIntent.roles = intent.roles.map((role) => {
      // We also need to patch "admin" to "instance_admin" due to a discrepancy in the backend types
      if (role === "ADMIN") {
        return "instance_admin";
      }
      return role.toLocaleLowerCase() as IntentDefinition["roles"][number];
    });
    patchedIntents[key] = patchedIntent;
  }
  return patchedIntents;
}

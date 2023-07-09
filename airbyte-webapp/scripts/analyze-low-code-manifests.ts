#! /usr/bin/env ts-node

import fs from "fs";

import chalk from "chalk";
import { glob } from "glob";
import { load } from "js-yaml";
import fetch from "node-fetch";

import { convertToBuilderFormValuesSync } from "../src/components/connectorBuilder/convertManifestToBuilderForm";
import { DeclarativeComponentSchema } from "../src/core/api/types/ConnectorManifest";

async function analyze(
  path: string
): Promise<
  | "no"
  | "no_custom_components"
  | "yaml_other"
  | "yaml_authenticator_parameters"
  | "yaml_datetime_format"
  | "yaml_authenticator_interpolation"
  | "yes"
> {
  console.log(`Analyze ${chalk.bold(path)}`);
  const manifest = load(fs.readFileSync(path, "utf8"));

  let response: { manifest: DeclarativeComponentSchema };
  try {
    response = await (
      await fetch("http://localhost:8003/v1/manifest/resolve", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          manifest,
        }),
      })
    ).json();
    const listTest = await fetch("http://localhost:8003/v1/streams/list", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        manifest,
        config: {},
      }),
    });
    const message = await listTest.json();
    if (listTest.status !== 200) {
      throw message.detail;
    }
  } catch (message) {
    console.log(chalk.red(`Resolving failed, won't work in builder at all: ${message}`));
    if ((message as string).includes("No module named")) {
      return "no_custom_components";
    }
    return "no";
  }

  try {
    convertToBuilderFormValuesSync(response.manifest, "");
  } catch (e) {
    const message = (e as { message: string }).message;
    console.log(chalk.yellow(`Converting to form values failed, won't work in UI but will in YAML: ${message}`));
    if (message.includes("authenticator does not match the first")) {
      return "yaml_authenticator_parameters";
    }
    if (message.includes("start_datetime or end_datetime are not set to a string")) {
      return "yaml_datetime_format";
    }
    if (message.includes("value must be of the form {{ config")) {
      return "yaml_authenticator_interpolation";
    }
    return "yaml_other";
  }

  console.log(chalk.green("Success, this yaml can be loaded in the builder UI"));
  return "yes";
}

async function run() {
  console.log(chalk.bold("Low code manifest analyzer"));
  console.log("This script is analyzing whether existing low code manifests can be used in the connector builder");

  const files = await glob("../../../airbyte/airbyte-integrations/connectors/**/manifest.yaml");

  if (files.length === 0) {
    console.log(
      `To use, check out the ${chalk.bold(
        "airbyte"
      )} repository next to the platform repository, start Airbyte locally using ${chalk.bold(
        'BASIC_AUTH_USERNAME="" BASIC_AUTH_PASSWORD="" VERSION=dev docker-compose --file ./docker-compose.yaml up'
      )}, then run this script.`
    );
    return;
  }

  console.log(`${files.length} manifests found`);
  const counts: Record<Awaited<ReturnType<typeof analyze>>, number> = {
    no: 0,
    no_custom_components: 0,
    yaml_authenticator_interpolation: 0,
    yaml_authenticator_parameters: 0,
    yaml_datetime_format: 0,
    yaml_other: 0,
    yes: 0,
  };

  for (const file of files) {
    const result = await analyze(file);
    counts[result]++;
  }

  console.log("");
  console.log("");
  console.log(chalk.bold("Summary"));
  console.log(chalk.red(`Not supported, custom components: ${counts.no_custom_components}`));
  console.log(chalk.red(`Not supported, other reasons: ${counts.no}`));
  console.log(
    chalk.yellow(
      `YAML only (authenticators are not consistent, could be caused by using $parameters in the authenticator): ${counts.yaml_authenticator_parameters}`
    )
  );
  console.log(
    chalk.yellow(
      `YAML only (uses wrong interpolation style in authenticator): ${counts.yaml_authenticator_interpolation}`
    )
  );
  console.log(chalk.yellow(`YAML only (uses MinMaxDatetime in incremental sync): ${counts.yaml_datetime_format}`));
  console.log(chalk.yellow(`YAML only (other reasons): ${counts.yaml_other}`));
  console.log(chalk.green(`Supports UI: ${counts.yes}`));
}

run();

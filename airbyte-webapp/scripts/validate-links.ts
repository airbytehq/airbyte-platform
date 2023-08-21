#! /usr/bin/env ts-node

import fetch from "node-fetch";

import { links } from "../src/core/utils/links";

const IGNORED_LINKS = [
  // Cloudflare in front prevents us from checking this without a real browser.
  // We assume that the URL won't change really, so it should be fine not validating it.
  "supportPortal",
  // Our demo link doesn't change but the demo tends to be unreliable down, so excluding
  // it's excluded from the link validation
  "demoLink",
];

async function run() {
  // Query all domains and wait for results
  const results = await Promise.allSettled(
    Object.entries(links).map(([key, url]) => {
      if (IGNORED_LINKS.includes(key)) {
        console.log(`⚬ [${key}] ${url} ignored for validation`);
        return Promise.resolve(true);
      }

      return fetch(url, { headers: { "user-agent": "ValidateLinksCheck" } })
        .then((resp) => {
          if (resp.status >= 200 && resp.status < 300) {
            // Only URLs returning a 200 status code are considered okay
            console.log(`✓ [${key}] ${url} returned HTTP ${resp.status}`);
          } else {
            // Everything else should fail this test
            console.error(`X [${key}] ${url} returned HTTP ${resp.status}`);
            return Promise.reject({ key, url });
          }
        })
        .catch((reason) => {
          console.error(`X [${key}] ${url} error fetching: ${JSON.stringify(reason, null, 2)}`);
          return Promise.reject({ key, url });
        });
    })
  );

  const failures = results.filter((result): result is PromiseRejectedResult => result.status === "rejected");

  if (failures.length > 0) {
    console.log(`\nThe following URLs were not successful: ${failures.map((r) => r.reason.key).join(", ")}`);
    process.exit(1);
  } else {
    console.log("\n✓ All URLs have been checked successfully.");
  }
}

run();

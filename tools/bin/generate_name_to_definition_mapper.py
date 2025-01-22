# Usage:
# python tools/bin/generate_name_to_definition_mapper.py

from datetime import datetime
from sys import exit
import json
import urllib.request

# URL to fetch the JSON from
url = "https://connectors.airbyte.com/files/registries/v0/oss_registry.json"

try:
    # Fetch the data from the URL
    with urllib.request.urlopen(url) as response:
        if response.status == 200:
            data = response.read()

            # Parse the JSON data
            parsed_data = json.loads(data)
        else:
            print(f"Failed to fetch data. HTTP Status Code: {response.status}")
            exit(1)
except Exception as e:
    print(f"An error occurred: {e}")
    exit(1)

sources = [
    {
        "name": source["documentationUrl"].split("/")[-1],
        "id": source["sourceDefinitionId"]
    }
    for source in parsed_data["sources"]
]
sources = sorted(sources, key=lambda source: source["name"])

destinations = [
    {
        "name": destination["documentationUrl"].split("/")[-1],
        "id": destination["destinationDefinitionId"]
    }
    for destination in parsed_data["destinations"]
]
destinations = sorted(destinations, key=lambda destination: destination["name"])

current_year = datetime.now().year

destination_name_to_definition_id = "\n    ".join([
    f"Pair(\"{ destination['name'] }\", UUID.fromString(\"{ destination['id'] }\")),"
    for destination in destinations
])

definition_id_to_destination_name = "\n    ".join([
    f"Pair(UUID.fromString(\"{ destination['id'] }\"), \"{ destination['name'] }\"),"
    for destination in destinations
])

source_name_to_definition_id = "\n    ".join([
    f"Pair(\"{ source['name'] }\", UUID.fromString(\"{ source['id'] }\")),"
    for source in sources
])

source_id_to_destination_name = "\n    ".join([
    f"Pair(UUID.fromString(\"{ source['id'] }\"), \"{ source['name'] }\"),"
    for source in sources
])

content = f"""/*
 * Copyright (c) 2020-{ current_year } Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.publicapi.mappers

import java.util.UUID

/*
 * The mappings in this file are used so that users can supply a connector name instead of definition id when referring
 * to specific connectors via the Airbyte API.
 */
val DESTINATION_NAME_TO_DEFINITION_ID: Map<String, UUID> =
  mapOf(
    { destination_name_to_definition_id }
  )

val DEFINITION_ID_TO_DESTINATION_NAME: Map<UUID, String> =
  mapOf(
    { definition_id_to_destination_name }
  )

val SOURCE_NAME_TO_DEFINITION_ID: Map<String, UUID> =
  mapOf(
    { source_name_to_definition_id }
  )

val DEFINITION_ID_TO_SOURCE_NAME: Map<UUID, String> =
  mapOf(
    { source_id_to_destination_name }
  )
"""

mapper_file = "airbyte-server/src/main/kotlin/io/airbyte/server/apis/publicapi/mappers/NameToDefinitionMapper.kt"
with open(mapper_file, "w") as f:
    f.write(content)

print(f"Generated {mapper_file}")

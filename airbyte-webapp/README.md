# airbyte-webapp

This module contains the Airbyte Webapp. It is a React app written in TypeScript.
The webapp compiles to static HTML, JavaScript and CSS, which is served (in OSS) via
a nginx in the airbyte-webapp docker image. This nginx also serves as the reverse proxy
for accessing the server APIs in other images.

## Building the webapp

You can build the webapp using Gradle in the root of the repository:

```sh
# Only compile and build the docker webapp image:
./gradlew :airbyte-webapp:assemble
# Build the webapp and additional artifacts and run tests:
./gradlew :airbyte-webapp:build
```

## Developing the webapp

For an instruction how to develop on the webapp, please refer to our [documentation](https://docs.airbyte.com/contributing-to-airbyte/developing-locally/#develop-on-airbyte-webapp).

Please also check our [styleguide](./STYLEGUIDE.md) for details around code styling and best-practises.

### Folder Structure

> 💡 Please note that this isn't representing the current state yet, but rather the targeted structure
we're trying to move towards.

**Services** folders are containing "services" which are usually React Context implementations 
(including their corresponding hooks to access them) or similar singleton type services.

**Utils** folders are containing any form of static utility functions or hooks not related to any service (otherwise they should go together with that service into a services folder).

```sh
src/
├ ui/ # All basic UI components
├ locales/ # Translation files
├ scss/ # SCSS variables, themes and utility files
├ types/ # TypeScript types needed across the code base
├ core/ # Core systems fundamental to the application
│ ├ api/ # API/request code
│ ├ config/ # Configuration system
│ ├ services/ # All core systems not belonging to a specific domain
│ └ utils/ # All core utilities not belonging to a specific domain
├ pages/ # Routing and page entry points (not all of the further components though)
│ ├ routes.tsx
│ ├ connections/
│ │ ├ AllConnectionPage/
│ │ └ # ...
│ └ # ...
├ area/ # Code for a specific domain of the webapp
│ ├ connection/
│ │ ├ services/ # Services for this area
│ │ ├ utils/ # Utils for this area
│ │ └ components/ # Components related to this area or for pages specific to this area
│ ├ connector/ # Has the same services/, utils/, components/ structure
│ ├ connectorBuilder/
│ ├ settings/
│ └ workspaces/
└ cloud/ # Cloud specific code (following a similar structure as above)
```

### Entrypoints

* `src/App.tsx` is the entrypoint into the OSS version of the webapp.
* `src/packages/cloud/App.tsx` is the entrypoint into the Cloud version of the webapp.

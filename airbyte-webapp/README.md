# airbyte-webapp

This module contains the Airbyte Webapp. It is a React app written in TypeScript.
The webapp compiles to static HTML, JavaScript and CSS, which is served (in OSS) via
a nginx in the airbyte-webapp docker image. This nginx also serves as the reverse proxy
for accessing the server APIs in other images.

## Building the webapp

You can build the webapp using Gradle in the root of the repository:

```sh
# Only compile and build the docker webapp image:
./gradlew :oss:airbyte-webapp:assemble
# Build the webapp and additional artifacts and run tests:
./gradlew :oss:airbyte-webapp:build
```

## Developing the webapp

For an instruction how to develop on the webapp, please refer to our [documentation](https://docs.airbyte.com/contributing-to-airbyte/developing-locally/#webapp-contributions).

Please also check our [styleguide](./STYLEGUIDE.md) for details around code styling and best-practises.

### Folder Structure

**Services** folders contain "services" which are usually React Context implementations
(including their corresponding hooks to access them) or similar singleton type services.

**Utils** folders contain any form of static utility functions or hooks not related to any service (otherwise they should go together with that service into a services folder).

```sh
src/
├ App.tsx                    # OSS entrypoint
│
├ core/                      # SHARED: Core systems used by both OSS & Cloud
│ ├ api/                     # API layer (generated code, hooks, types)
│ ├ config/                  # Configuration and environment
│ ├ domain/                  # Domain models (catalog, connector types)
│ ├ errors/                  # Error utilities
│ ├ form/                    # Form utilities
│ ├ jsonSchema/              # JSON schema utilities
│ ├ services/                # Cross-domain services
│ │ ├ analytics/
│ │ ├ auth/
│ │ ├ connectorBuilder/
│ │ ├ features/              # Feature flags
│ │ ├ i18n/
│ │ ├ ConfirmationModal/
│ │ ├ Experiment/
│ │ ├ FormChangeTracker/
│ │ ├ Health/
│ │ ├ Modal/
│ │ ├ navigation/
│ │ ├ Notification/
│ │ └ ui/
│ └ utils/                   # Core utilities & hooks
│
├ area/                      # SHARED: Domain areas used by both OSS & Cloud
│ ├ auth/                    # Authentication domain
│ │ └ components/
│ ├ connection/              # Connection domain
│ │ ├ components/            # Connection-specific components (forms, tables, status)
│ │ ├ types/
│ │ └ utils/
│ ├ connector/               # Connector domain
│ │ ├ components/            # Connector forms, cards, documentation
│ │ ├ types/
│ │ └ utils/
│ ├ connectorBuilder/        # Connector Builder domain
│ │ └ components/
│ ├ dataActivation/          # Data activation domain (reverse ETL)
│ │ ├ components/
│ │ ├ types/
│ │ └ utils/
│ ├ layout/                  # Layout components (MainLayout, SideBar)
│ ├ organization/            # Organization domain
│ │ └ components/
│ ├ settings/                # Settings domain
│ │ └ components/
│ └ workspace/               # Workspace domain
│   ├ components/
│   └ utils/
│
├ components/                # SHARED: Reusable UI components
│ └ ui/                      # UI primitives ONLY (Button, Modal, Input, etc.)
│
├ pages/                     # SHARED: Route handlers for both OSS & Cloud
│ ├ routes.tsx
│ ├ connections/
│ ├ connectorBuilder/
│ ├ destination/
│ ├ source/
│ └ ...
│
├ cloud/                     # CLOUD-ONLY: Cloud-specific additions
│ ├ App.tsx                  # Cloud entrypoint
│ ├ cloudRoutes.tsx
│ ├ area/                    # Cloud-specific domain areas
│ │ └ billing/
│ ├ components/              # Cloud-specific components
│ ├ services/                # Cloud-specific services (auth, third-party)
│ │ ├ auth/
│ │ └ thirdParty/
│ └ views/                   # Cloud-specific page views
│
├ locales/                   # SHARED: i18n translation files
├ scss/                      # SHARED: Global styles, variables, themes
├ test-utils/                # SHARED: Test utilities and mock data
└ types/                     # SHARED: Global TypeScript types
```

**Key principles:**
- `core/`, `area/`, `components/ui/`, `pages/` are shared between OSS and Cloud
- `cloud/` only contains Cloud-specific additions (billing, auth, integrations)
- Domain-specific code lives in `area/`
- Only basic reusable UI components in `components/ui/`

### OSS/Cloud Sharing Model

Most of the codebase is **shared** between OSS and Cloud versions. Cloud adds features on top of the shared foundation:

**SHARED Code** (used by both OSS & Cloud):
- `src/core/` - Core systems (API, config, services, utils)
- `src/area/` - All domain areas (connection, connector, workspace, etc.)
- `src/components/ui/` - Reusable UI primitives
- `src/pages/` - Route handlers and page components
- `src/locales/`, `src/scss/`, `src/test-utils/`, `src/types/` - Supporting code

**CLOUD-ONLY Code** (additions specific to Cloud):
- `src/cloud/App.tsx` - Cloud entrypoint with additional providers
- `src/cloud/area/billing/` - Billing domain (subscriptions, usage, payment)
- `src/cloud/services/auth/` - Cloud authentication (OAuth, session management)
- `src/cloud/services/thirdParty/` - Third-party integrations (Zendesk, LaunchDarkly, analytics)
- `src/cloud/components/` - Cloud-specific UI components
- `src/cloud/views/` - Cloud-specific page views

**How it works:**
- Both versions share the same core architecture and domain code
- OSS entrypoint (`src/App.tsx`) uses `OSSAuthService` and default features
- Cloud entrypoint (`src/cloud/App.tsx`) uses `CloudAuthService` and adds Cloud-specific providers
- Cloud can import and extend shared code, but shared code cannot import from `cloud/`

### Quick Reference

**Where should I put...?**

| What you're adding | Where it goes |
|-------------------|--------------|
| New connection feature | `src/area/connection/` |
| New connector feature | `src/area/connector/` |
| Workspace management | `src/area/workspace/` |
| Settings feature | `src/area/settings/` |
| Reusable UI component (Button, Modal) | `src/components/ui/` |
| New API hook | `src/core/api/hooks/` |
| New page/route | `src/pages/` |
| Cross-domain service | `src/core/services/` |
| Analytics code | `src/core/services/analytics/` |
| Feature flags | `src/core/services/features/` |
| Billing feature (Cloud) | `src/cloud/area/billing/` |
| Cloud authentication | `src/cloud/services/auth/` |
| Third-party integration (Cloud) | `src/cloud/services/thirdParty/` |

### Directory Reference

This table provides detailed guidance for navigating the codebase:

| Directory | Shared/Cloud | Purpose | Examples | When to Use |
|-----------|--------------|---------|----------|-------------|
| `src/core/api/` | SHARED | API communication layer with auto-generated clients from OpenAPI specs | Generated clients, `hooks/`, `types/`, `apis.ts` | All API-related code (hooks, types, config) |
| `src/core/config/` | SHARED | Application configuration and environment variables | Config providers, environment setup | App-level configuration |
| `src/core/services/` | SHARED | Cross-domain business logic services | `analytics/`, `auth/`, `features/`, `i18n/`, `Modal/`, `Notification/` | Services used across multiple domains |
| `src/core/utils/` | SHARED | Shared utility functions and hooks | Common hooks, formatters, validators | Utilities used across the app |
| `src/area/connection/` | SHARED | Connection domain - data sync pipelines | `components/ConnectionStatusIndicator/`, sync scheduling, status management | Connection-related features |
| `src/area/connector/` | SHARED | Connector domain - sources and destinations | Connector forms, cards, documentation, discovery | Connector management features |
| `src/area/workspace/` | SHARED | Workspace domain - workspace management | Workspace settings, permissions, users | Workspace-related features |
| `src/area/organization/` | SHARED | Organization domain - multi-workspace management | Organization settings, workspace lists | Organization features |
| `src/area/settings/` | SHARED | Settings domain - application settings | Settings pages, configuration forms | Settings features |
| `src/area/dataActivation/` | SHARED | Data activation domain - reverse ETL | Data activation connections and management | Reverse ETL features |
| `src/components/ui/` | SHARED | Reusable UI primitives ONLY | `Button/`, `Modal/`, `Input/`, `Card/`, `Badge/` | Basic UI components (not domain-specific) |
| `src/pages/` | SHARED | Route handlers and page-level components | `routes.tsx`, `connections/`, `source/`, `destination/` | Page routing and top-level views |
| `src/cloud/area/billing/` | CLOUD-ONLY | Billing domain - subscriptions and payments | Billing pages, usage tracking, payment forms | Cloud billing features |
| `src/cloud/services/auth/` | CLOUD-ONLY | Cloud authentication service | OAuth flows, session management, CloudAuthService | Cloud-specific auth |
| `src/cloud/services/thirdParty/` | CLOUD-ONLY | Third-party service integrations | Zendesk, LaunchDarkly, analytics providers | External service integration |

### Decision Trees: Where to Put Code

These flowcharts help you quickly determine where new code should go:

#### For Components

```
New component?
├─ Is it a basic UI primitive (Button, Input, Modal, Card, etc.)?
│  └─ YES → src/components/ui/
│
├─ Is it Cloud-only (billing UI, Zendesk widget)?
│  └─ YES → src/cloud/components/ or src/cloud/area/{domain}/components/
│
├─ Is it specific to a domain (Connection, Connector, Workspace, Organization)?
│  └─ YES → src/area/{domain}/components/
│
└─ Is it a page/route handler?
   └─ YES → src/pages/
```

#### For Services

```
New service?
├─ Is it Cloud-only (CloudAuth, third-party integrations)?
│  └─ YES → src/cloud/services/
│
├─ Is it domain-specific (used only within one domain)?
│  └─ YES → src/area/{domain}/services/
│
└─ Is it cross-domain/used by multiple areas?
   └─ YES → src/core/services/
```

#### For Utilities and Hooks

```
New utility or hook?
├─ Is it Cloud-only?
│  └─ YES → src/cloud/area/{domain}/utils/ or src/cloud/services/
│
├─ Is it domain-specific (used only within one domain)?
│  └─ YES → src/area/{domain}/utils/
│
└─ Is it shared across domains?
   └─ YES → src/core/utils/
```

### Import Rules & Restrictions

The codebase enforces import restrictions via ESLint to maintain clean architecture:

#### API Import Isolation

**Rule:** Outside of `core/api/`, you can only import from:
- `core/api` (index exports)
- `core/api/cloud` (Cloud API exports)
- `core/api/types/*` (type definitions)

**Why:** This prevents coupling to internal API implementation details and keeps the API layer maintainable.

**Example:**
```typescript
// ✅ CORRECT
import { useConnection } from "core/api";
import type { ConnectionRead } from "core/api/types/AirbyteClient";

// ❌ WRONG
import { getConnection } from "core/api/generated/AirbyteClient";
```

See `core/api/README.md` for detailed API layer documentation.

#### Other Import Restrictions

**Local Storage:**
```typescript
// ✅ CORRECT
import { useLocalStorage } from "core/utils/useLocalStorage";

// ❌ WRONG
import { useLocalStorage } from "react-use";
```

**Lodash:**
```typescript
// ✅ CORRECT
import get from "lodash/get";

// ❌ WRONG
import { get } from "lodash";
```

**Links:**
```typescript
// ✅ CORRECT
import { Link } from "components/ui/Link";

// ❌ WRONG
import { Link } from "react-router-dom";
```

### Architecture Rationale

Understanding why the structure exists helps you work with it effectively:

#### Domain-Driven Design

**Why `area/` domains instead of feature folders?**

The codebase organizes code by business domain (connection, connector, workspace) rather than by technical feature. This provides:
- **Clear ownership:** Each domain has a clear scope and responsibility
- **Better scalability:** Domains can grow independently without affecting others
- **Easier navigation:** Finding code related to "connections" means looking in `area/connection/`
- **Natural boundaries:** Domains align with how users think about the product

#### OSS/Cloud Sharing

**Why is most code shared with Cloud as additions?**

This architecture minimizes code duplication and maintenance burden:
- **Single source of truth:** Bug fixes and features benefit both versions
- **Consistent UX:** Users get a similar experience across OSS and Cloud
- **Easier testing:** Most code can be tested once for both versions
- **Clear separation:** Cloud-specific code is isolated in `cloud/` directory

#### API Isolation

**Why are API imports restricted?**

The API layer is auto-generated from OpenAPI specs. Restricting imports:
- **Prevents coupling:** Internal API structure can change without breaking consumers
- **Enables regeneration:** Generated code can be updated without fixing imports across the codebase
- **Enforces patterns:** Encourages using React Query hooks rather than direct API calls
- **Improves maintainability:** Clear contract between API layer and the rest of the app

#### Component Separation

**Why are UI primitives separate from domain components?**

Separating `components/ui/` from `area/{domain}/components/` provides:
- **Reusability:** UI primitives can be used across all domains
- **Design consistency:** All domains use the same base components
- **Clear responsibility:** UI primitives handle display, domain components handle business logic
- **Easier styling:** Global design system changes happen in one place

#### Service Organization

**Why split services between `core/` and domain areas?**

This separation clarifies service scope:
- **Core services:** Used across multiple domains (analytics, auth, notifications)
- **Domain services:** Specific to one domain's business logic
- **Prevents bloat:** Core doesn't become a dumping ground for all services
- **Enables isolation:** Domain services can be tested independently

### Common Development Patterns

#### Creating a New Domain Area

If you need to add a new domain (e.g., `src/area/newDomain/`):

```
src/area/newDomain/
├── components/           # Domain-specific components
│   └── NewDomainCard/
│       ├── NewDomainCard.tsx
│       ├── NewDomainCard.module.scss
│       ├── NewDomainCard.test.tsx
│       └── index.ts
├── types/                # Domain-specific types (if needed)
│   └── index.ts
└── utils/                # Domain-specific utilities (if needed)
    └── domainHelpers.ts
```

#### Adding Cloud-Specific Features

For Cloud-only features (e.g., new billing functionality):

```
src/cloud/area/billing/
├── components/
│   └── NewBillingFeature/
│       ├── NewBillingFeature.tsx
│       ├── NewBillingFeature.module.scss
│       └── index.ts
└── utils/
    └── billingHelpers.ts
```

#### Component Organization Best Practices

Each component follows a consistent structure:

```
ComponentName/
├── ComponentName.tsx              # Main component code
├── ComponentName.module.scss      # Scoped styles
├── ComponentName.test.tsx         # Unit tests
├── ComponentName.stories.tsx      # Storybook stories (optional)
└── index.ts                       # Re-export for clean imports
```

**Real example:** `src/area/connection/components/ConnectionStatusIndicator/`

#### API Hook Pattern

Manual React Query hooks wrap generated API functions:

```typescript
// In src/core/api/hooks/connections.ts
export const useConnection = (connectionId: string) => {
  const requestOptions = useRequestOptions();
  return useSuspenseQuery(
    connectionKeys.detail(connectionId),
    () => getConnection({ connectionId }, requestOptions)
  );
};
```

This pattern provides:
- Type-safe API calls
- Automatic caching and revalidation
- Consistent error handling
- Request authentication

### Entrypoints

The webapp has two entrypoints that share most code but configure services differently:

**OSS Entrypoint:** `src/App.tsx`
- Uses `OSSAuthService` for local authentication
- Default feature flags from `src/core/services/features/constants.ts`
- Standard service providers: Analytics, FeatureService, Modal, Notification, etc.

**Cloud Entrypoint:** `src/cloud/App.tsx`
- Uses `CloudAuthService` for OAuth authentication
- Cloud-specific feature flags from `defaultCloudFeatures`
- Additional Cloud providers: HockeyStack, PostHog, Zendesk, LaunchDarkly
- Extends OSS providers with Cloud-specific functionality

Both entrypoints load the same routing configuration and shared components, with Cloud adding extra routes via `src/cloud/cloudRoutes.tsx`.

## Testing the webapp

### Unit tests with Jest

To run unit tests interactively, use `pnpm test`. To start a one-off headless run, use
`pnpm test:ci` instead. Unit test files are located in the same directory as the module
they're testing, using the `.test.ts` extension.

### End-to-end (e2e) tests with Playwright

End-to-end tests are written using [Playwright](https://playwright.dev/). The test suite can be found in the `/playwright` directory. For detailed instructions on running and debugging Playwright tests, please refer to the [Playwright README](/playwright/README.md).

#### Using local k8s and `make` (recommended)

##### Running an interactive Playwright session

The most useful way to run tests locally is in Playwright's UI mode, which lets you select which tests and browser to run, see the browser in action, and use the Playwright inspector for debugging.

1. Build and start the OSS backend by running either `make dev.up.oss` or `make build.oss deploy.oss`
2. Run `make test.e2e.oss.open` to start Playwright in UI mode with all dependencies configured.

##### Reproducing CI test results

This triggers headless runs: you won't have a live browser to interact with, just terminal output. This can be useful for debugging the occasional e2e failures that are not reproducible in the typical browser mode. This will print the same output you would see on CI.

1. Build and start the OSS backend by running either `make dev.up.oss` or `make build.oss deploy.oss`
2. Run `make test.e2e.oss`. This will take care of running dependency scripts to spin up a source and destination db and dummy API, and run the Playwright tests.

#### Test setup

When the tests are run as described above, the platform under test is started via kubernetes in the ab namespace. To test connections from real sources and destinations, additional docker containers are started for hosting these. For basic connections, additional postgres instances are started (`createdbsource` and `createdbdestination`).

For testing the connector builder UI, a dummy api server based on a node script is started (`createdummyapi`). It is providing a simple http API with bearer authentication returning a few records of hardcoded data. By running it in the internal airbyte network, the connector builder server can access it under its container name.

The tests instrument a browser to test the full functionality of Airbyte from the frontend, so other components of the platform (scheduler, worker, connector builder server) are also tested in a rudimentary way.

##### Caveats

1. due to an upstream bug with `vite preview` dev servers, running headless tests against an optimized, pre-bundled frontend build will always signal failure with a non-zero exit code, even if all tests pass
2. one early tester reported some cross-browser instability, where tests run with `pnpm test:dev` failed on Chrome but not Electron

# Airbyte Feature Flag

A comprehensive feature flag management system for Airbyte, supporting multiple backend providers and context-aware flag evaluation.

## Overview

The `airbyte-featureflag` module provides a unified interface for managing feature flags across different Airbyte environments. It supports multiple backend providers (LaunchDarkly, config files, and a custom feature flag service) with context-aware evaluation based on users, workspaces, organizations, and other entities.

## Core Components

### Flag Types

Feature flags are defined using a hierarchical type system:

- **`Flag<T>`**: Base sealed class for all feature flags
- **`Temporary<T>`**: Flags intended to be removed after rollout completion
- **`Permanent<T>`**: Flags intended to exist long-term as configuration toggles
- **`EnvVar`**: Environment variable-based flags (transitional, being migrated to proper flags)

#### Flag Examples

```kotlin
// Temporary feature flag
object FieldSelectionEnabled : Temporary<Boolean>(
    key = "connection.columnSelection", 
    default = true
)

// Permanent configuration flag
object HeartbeatMaxSecondsBetweenMessages : Permanent<String>(
    key = "heartbeat-max-seconds-between-messages", 
    default = "10800"
)

// Environment variable flag (deprecated pattern)
object LogConnectorMessages : EnvVar(envVar = "LOG_CONNECTOR_MESSAGES")
```

### Client Implementations

The module provides multiple `FeatureFlagClient` implementations:

#### 1. **ConfigFileClient** (Default)
- Reads flags from YAML configuration files
- Supports file watching for real-time updates
- Context-aware flag evaluation
- Fallback for when no external service is configured

#### 2. **LaunchDarklyClient**
- Integrates with LaunchDarkly service
- Production-ready with advanced targeting
- Requires API key configuration
- Supports context interception

#### 3. **FeatureFlagServiceClient**
- Custom Airbyte feature flag service
- HTTP-based flag evaluation
- Configurable base URL

#### 4. **TestClient**
- Mock implementation for testing
- Override any flag values in tests
- Compatible with Micronaut's `@MockBean`

### Context System

Feature flags are evaluated within contexts representing different entities:

```kotlin
// Core contexts
val user = User(UUID.fromString("..."), EmailAttribute("user@example.com"))
val workspace = Workspace(UUID.fromString("..."))
val organization = Organization(UUID.fromString("..."))
val connection = Connection(UUID.fromString("..."))

// Multi-context evaluation
val context = Multi(listOf(user, workspace, organization))
val flagValue = client.boolVariation(SomeFlag, context)
```

#### Available Context Types

- **`User`**: User-specific targeting with email attributes
- **`Workspace`**: Workspace-scoped feature rollouts
- **`Organization`**: Organization-level feature control
- **`Connection`**: Connection-specific feature flags
- **`Source`/`Destination`**: Actor-specific targeting
- **`SourceDefinition`/`DestinationDefinition`**: Connector-type targeting
- **`Dataplane`/`DataplaneGroup`**: Infrastructure targeting
- **`CloudProvider`/`GeographicRegion`**: Geographic targeting
- **`Multi`**: Combine multiple contexts for complex targeting

## Configuration

### Client Selection

Configure which feature flag client to use:

```yaml
airbyte:
  feature-flag:
    client: "launchdarkly"  # or "ffs" or any other value for ConfigFileClient
```

### LaunchDarkly Configuration

```yaml
airbyte:
  feature-flag:
    client: "launchdarkly"
    api-key: "your-launchdarkly-api-key"
```

### Config File Configuration

```yaml
airbyte:
  feature-flag:
    path: "/path/to/flags.yml"
```

Example `flags.yml`:

```yaml
flags:
  - name: feature-enabled
    serve: true
    context:
      - type: "workspace"
        include:
          - "workspace-uuid-1"
          - "workspace-uuid-2"
        serve: false
      - type: "organization"
        include:
          - "org-uuid-1"
        serve: true
```

### Feature Flag Service Configuration

```yaml
airbyte:
  feature-flag:
    client: "ffs"
    base-url: "https://feature-flag-service.example.com"
```

## Usage Examples

### Basic Flag Evaluation

```kotlin
@Inject
lateinit var featureFlagClient: FeatureFlagClient

fun someBusinessLogic() {
    val context = Workspace(workspaceId)
    
    if (featureFlagClient.boolVariation(FieldSelectionEnabled, context)) {
        // New field selection behavior
        enableFieldSelection()
    } else {
        // Legacy behavior
        useDefaultFields()
    }
}
```

### Multi-Context Evaluation

```kotlin
val user = User(userId, EmailAttribute("user@company.com"))
val workspace = Workspace(workspaceId)
val connection = Connection(connectionId)

val context = Multi(listOf(user, workspace, connection))
val timeoutSeconds = featureFlagClient.intVariation(DestinationTimeoutSeconds, context)
```

### Testing with Mock Flags

```kotlin
@MicronautTest
class MyServiceTest {
    
    @MockBean(FeatureFlagClient::class)
    fun mockFeatureFlagClient(): TestClient {
        return TestClient(mapOf(
            "field-selection-enabled" to true,
            "destination-timeout-seconds" to 300
        ))
    }
    
    @Test
    fun testWithFeatureEnabled() {
        // Test logic with feature flag enabled
    }
}
```

### Environment Variable Flags (Legacy)

```kotlin
// Custom environment variable flag with context-aware logic
object CustomEnvFlag : EnvVar("CUSTOM_FEATURE") {
    override fun enabled(ctx: Context): Boolean {
        val envValue = fetcher(key)
        return when (ctx) {
            is Workspace -> {
                // Custom logic for workspace context
                envValue?.split(",")?.contains(ctx.key) ?: default
            }
            else -> super.enabled(ctx)
        }
    }
}
```

## File Watching (ConfigFileClient)

The `ConfigFileClient` automatically watches the configuration file for changes:

- Uses Java NIO `WatchService` for efficient file monitoring
- Updates flag values in real-time without restart
- Thread-safe with read-write locks
- Handles file creation, modification, and error scenarios

## Architecture Benefits

### Type Safety
- Compile-time flag key validation
- Type-safe flag value access
- Clear distinction between temporary and permanent flags

### Flexibility
- Multiple backend providers
- Pluggable client implementations
- Context-aware evaluation
- Real-time configuration updates

### Testing
- Built-in test client
- Easy flag override in tests
- Micronaut integration support

### Migration Support
- Gradual migration from environment variables
- Backward compatibility
- Clear migration path defined

## Implementation Details

### Micronaut Integration

The module uses Micronaut's dependency injection with conditional bean loading:

```kotlin
@Singleton
@Requires(property = CONFIG_FF_CLIENT, value = CONFIG_FF_CLIENT_VAL_LAUNCHDARKLY)
class LaunchDarklyClient(private val client: LDClient) : FeatureFlagClient
```

### Thread Safety

- `ConfigFileClient` uses `ReentrantReadWriteLock` for thread-safe flag updates
- File watching runs on a low-priority daemon thread
- All clients are designed to be thread-safe for concurrent access

### Error Handling

- Graceful fallback to default values when evaluation fails
- Comprehensive logging for debugging
- Non-intrusive error handling that won't break application flow

## Dependencies

- **LaunchDarkly SDK**: External feature flag service integration
- **Jackson**: YAML configuration file parsing
- **Micronaut**: Dependency injection and configuration
- **OkHttp**: HTTP client for feature flag service calls

## Migration Guide

### From Environment Variables

1. Define a proper `Temporary` or `Permanent` flag:
```kotlin
object MyFeature : Temporary<Boolean>(key = "my.feature", default = false)
```

2. Replace environment variable usage:
```kotlin
// Old
val enabled = System.getenv("MY_FEATURE")?.toBoolean() ?: false

// New
val enabled = featureFlagClient.boolVariation(MyFeature, context)
```

3. Update configuration to use proper feature flag backend

### Adding New Flags

1. Define the flag in `FlagDefinitions.kt`:
```kotlin
object NewFeature : Temporary<Boolean>(key = "new.feature", default = false)
```

2. Use throughout codebase:
```kotlin
if (featureFlagClient.boolVariation(NewFeature, context)) {
    // New behavior
}
```

3. Configure in your chosen backend (LaunchDarkly, config file, etc.)

## Current Flags

The module includes 100+ predefined feature flags covering:
- **Connection Management**: Field selection, sync scheduling, timeout handling
- **Connector Management**: OAuth, breaking changes, resource requirements
- **Platform Features**: Async profiling, workload management, secret handling
- **Billing & Licensing**: Usage tracking, enterprise features
- **Development & Testing**: Debug modes, development overrides

See `FlagDefinitions.kt` for the complete list of available flags.
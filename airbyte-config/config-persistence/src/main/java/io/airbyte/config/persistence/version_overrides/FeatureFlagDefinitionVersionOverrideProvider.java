/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

/**
 * Defines the interface for a class that can provide overrides for actor definition versions based
 * on feature flags. This interface is used to allow for a different implementation of the FF
 * override provider in Cloud and OSS, namely for adding additional contexts that are only available
 * in Cloud.
 */
public interface FeatureFlagDefinitionVersionOverrideProvider extends DefinitionVersionOverrideProvider {}

/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

/**
 * Defines the interface for a class that can provide static overrides for actor definition
 * versions. This is used to allow for a different implementation of the override provider in Cloud
 * and OSS, namely for changing the file path that the overrides are stored in.
 */
public interface LocalDefinitionVersionOverrideProvider extends DefinitionVersionOverrideProvider {}

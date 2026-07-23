import { hasAnyTouchedNestedField, mergePreservingTouchedFields } from "./mergePreservingTouchedFields";

describe("hasAnyTouchedNestedField", () => {
  it("returns true when a nested field is touched", () => {
    const touchedFields = new Set(["connectionConfiguration.tunnel_method.tunnel_user_password"]);
    expect(hasAnyTouchedNestedField("connectionConfiguration.tunnel_method", touchedFields)).toBe(true);
  });

  it("returns false when no nested fields are touched", () => {
    const touchedFields = new Set(["connectionConfiguration.password"]);
    expect(hasAnyTouchedNestedField("connectionConfiguration.tunnel_method", touchedFields)).toBe(false);
  });

  it("returns false when the exact field is touched but no nested fields", () => {
    const touchedFields = new Set(["connectionConfiguration.tunnel_method"]);
    expect(hasAnyTouchedNestedField("connectionConfiguration.tunnel_method", touchedFields)).toBe(false);
  });

  it("returns true for deeply nested fields", () => {
    const touchedFields = new Set(["connectionConfiguration.a.b.c.d.e"]);
    expect(hasAnyTouchedNestedField("connectionConfiguration.a", touchedFields)).toBe(true);
    expect(hasAnyTouchedNestedField("connectionConfiguration.a.b", touchedFields)).toBe(true);
    expect(hasAnyTouchedNestedField("connectionConfiguration.a.b.c", touchedFields)).toBe(true);
    expect(hasAnyTouchedNestedField("connectionConfiguration.a.b.c.d", touchedFields)).toBe(true);
  });
});

describe("mergePreservingTouchedFields", () => {
  const mockGetCurrentValue = (storage: Record<string, unknown>) => (path: string) => {
    const keys = path.split(".");
    let current: unknown = storage;
    for (const key of keys) {
      current = (current as Record<string, unknown>)?.[key];
    }
    return current;
  };

  describe("top-level fields", () => {
    it("preserves a single touched top-level field", () => {
      const currentStorage = {
        connectionConfiguration: {
          host: "localhost",
          password: "original_password",
          port: 5432,
        },
      };

      const newConfig = {
        host: "newhost",
        password: "new_password",
        port: 5433,
      };

      const touchedFields = new Set(["connectionConfiguration.password"]);

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        host: "newhost",
        password: "original_password", // Preserved
        port: 5433,
      });
    });

    it("merges untouched fields normally", () => {
      const currentStorage = {
        connectionConfiguration: {
          host: "localhost",
          port: 5432,
        },
      };

      const newConfig = {
        host: "newhost",
        port: 5433,
        database: "newdb",
      };

      const touchedFields = new Set<string>();

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        host: "newhost",
        port: 5433,
        database: "newdb",
      });
    });
  });

  describe("nested fields (2-3 levels)", () => {
    it("preserves nested touched fields", () => {
      const currentStorage = {
        connectionConfiguration: {
          host: "localhost",
          tunnel_method: {
            tunnel_host: "tunnel.example.com",
            tunnel_user_password: "original_tunnel_password",
          },
        },
      };

      const newConfig = {
        host: "newhost",
        tunnel_method: {
          tunnel_host: "new-tunnel.example.com",
          tunnel_user_password: "new_tunnel_password",
        },
      };

      const touchedFields = new Set(["connectionConfiguration.tunnel_method.tunnel_user_password"]);

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        host: "newhost",
        tunnel_method: {
          tunnel_host: "new-tunnel.example.com",
          tunnel_user_password: "original_tunnel_password", // Preserved
        },
      });
    });

    it("handles partial nested object merging", () => {
      const currentStorage = {
        connectionConfiguration: {
          auth: {
            username: "user",
            password: "original_password",
            token: "original_token",
          },
        },
      };

      const newConfig = {
        auth: {
          username: "newuser",
          password: "new_password",
        },
      };

      const touchedFields = new Set(["connectionConfiguration.auth.password"]);

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        auth: {
          username: "newuser",
          password: "original_password", // Preserved
          token: "original_token", // Preserved from current
        },
      });
    });
  });

  describe("deeply nested fields (4+ levels)", () => {
    it("preserves deeply nested touched fields", () => {
      const currentStorage = {
        connectionConfiguration: {
          level1: {
            level2: {
              level3: {
                level4: {
                  secret: "original_secret",
                  other: "original_other",
                },
              },
            },
          },
        },
      };

      const newConfig = {
        level1: {
          level2: {
            level3: {
              level4: {
                secret: "new_secret",
                other: "new_other",
              },
            },
          },
        },
      };

      const touchedFields = new Set(["connectionConfiguration.level1.level2.level3.level4.secret"]);

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        level1: {
          level2: {
            level3: {
              level4: {
                secret: "original_secret", // Preserved
                other: "new_other",
              },
            },
          },
        },
      });
    });
  });

  describe("parent field touched", () => {
    it("preserves all children when parent object is touched", () => {
      const currentStorage = {
        connectionConfiguration: {
          tunnel_method: {
            tunnel_host: "original-tunnel.example.com",
            tunnel_port: 22,
            tunnel_user: "original_user",
            tunnel_user_password: "original_password",
          },
        },
      };

      const newConfig = {
        tunnel_method: {
          tunnel_host: "new-tunnel.example.com",
          tunnel_port: 2222,
          tunnel_user: "new_user",
          tunnel_user_password: "new_password",
        },
      };

      const touchedFields = new Set(["connectionConfiguration.tunnel_method"]);

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        tunnel_method: {
          tunnel_host: "original-tunnel.example.com",
          tunnel_port: 22,
          tunnel_user: "original_user",
          tunnel_user_password: "original_password",
        },
      });
    });
  });

  describe("arrays", () => {
    it("treats arrays as atomic values", () => {
      const currentStorage = {
        connectionConfiguration: {
          tags: ["original", "tags"],
        },
      };

      const newConfig = {
        tags: ["new", "tags", "list"],
      };

      const touchedFields = new Set<string>();

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        tags: ["new", "tags", "list"],
      });
    });

    it("preserves arrays when touched", () => {
      const currentStorage = {
        connectionConfiguration: {
          tags: ["original", "tags"],
        },
      };

      const newConfig = {
        tags: ["new", "tags"],
      };

      const touchedFields = new Set(["connectionConfiguration.tags"]);

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        tags: ["original", "tags"],
      });
    });
  });

  describe("null/undefined values", () => {
    it("handles null values", () => {
      const currentStorage = {
        connectionConfiguration: {
          optional_field: null,
        },
      };

      const newConfig = {
        optional_field: "new_value",
      };

      const touchedFields = new Set<string>();

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        optional_field: "new_value",
      });
    });

    it("handles undefined values", () => {
      const currentStorage = {
        connectionConfiguration: {
          optional_field: undefined,
        },
      };

      const newConfig = {
        optional_field: "new_value",
      };

      const touchedFields = new Set<string>();

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        optional_field: "new_value",
      });
    });

    it("preserves null when field is touched", () => {
      const currentStorage = {
        connectionConfiguration: {
          optional_field: null,
        },
      };

      const newConfig = {
        optional_field: "new_value",
      };

      const touchedFields = new Set(["connectionConfiguration.optional_field"]);

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        optional_field: null,
      });
    });
  });

  describe("primitives", () => {
    it("handles string values", () => {
      const currentStorage = {
        connectionConfiguration: {
          name: "original",
        },
      };

      const newConfig = {
        name: "new",
      };

      const touchedFields = new Set<string>();

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        name: "new",
      });
    });

    it("handles number values", () => {
      const currentStorage = {
        connectionConfiguration: {
          port: 5432,
        },
      };

      const newConfig = {
        port: 3306,
      };

      const touchedFields = new Set<string>();

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        port: 3306,
      });
    });

    it("handles boolean values", () => {
      const currentStorage = {
        connectionConfiguration: {
          ssl_enabled: false,
        },
      };

      const newConfig = {
        ssl_enabled: true,
      };

      const touchedFields = new Set<string>();

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        ssl_enabled: true,
      });
    });
  });

  describe("complex real-world scenario", () => {
    it("handles Postgres connector with SSH tunnel and multiple touched fields", () => {
      const currentStorage = {
        connectionConfiguration: {
          host: "localhost",
          port: 5432,
          database: "testdb",
          username: "testuser",
          password: "user_entered_password",
          tunnel_method: {
            tunnel_method: "SSH_PASSWORD_AUTH",
            tunnel_host: "tunnel.example.com",
            tunnel_port: 22,
            tunnel_user: "tunnel_user",
            tunnel_user_password: "user_entered_tunnel_password",
          },
        },
      };

      const newConfig = {
        host: "localhost",
        port: 5432,
        database: "testdb",
        username: "testuser",
        password: "***", // Agent trying to set placeholder
        tunnel_method: {
          tunnel_method: "SSH_PASSWORD_AUTH",
          tunnel_host: "tunnel.example.com",
          tunnel_port: 22,
          tunnel_user: "tunnel_user",
          tunnel_user_password: "***", // Agent trying to set placeholder
        },
      };

      const touchedFields = new Set([
        "connectionConfiguration.password",
        "connectionConfiguration.tunnel_method.tunnel_user_password",
      ]);

      const result = mergePreservingTouchedFields(
        "connectionConfiguration",
        newConfig,
        mockGetCurrentValue(currentStorage),
        touchedFields
      );

      expect(result).toEqual({
        host: "localhost",
        port: 5432,
        database: "testdb",
        username: "testuser",
        password: "user_entered_password", // Preserved
        tunnel_method: {
          tunnel_method: "SSH_PASSWORD_AUTH",
          tunnel_host: "tunnel.example.com",
          tunnel_port: 22,
          tunnel_user: "tunnel_user",
          tunnel_user_password: "user_entered_tunnel_password", // Preserved
        },
      });
    });
  });
});

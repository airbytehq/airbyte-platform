import { findChangedFieldPaths } from "./findChangedFieldPaths";

describe("findChangedFieldPaths", () => {
  describe("no changes", () => {
    it("returns empty array when configurations are identical", () => {
      const oldConfig = {
        host: "localhost",
        port: 5432,
        database: "testdb",
      };

      const newConfig = {
        host: "localhost",
        port: 5432,
        database: "testdb",
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual([]);
    });

    it("returns empty array for empty objects", () => {
      const result = findChangedFieldPaths({}, {});
      expect(result).toEqual([]);
    });
  });

  describe("top-level changes", () => {
    it("detects single field change", () => {
      const oldConfig = {
        host: "localhost",
        port: 5432,
      };

      const newConfig = {
        host: "newhost",
        port: 5432,
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual(["connectionConfiguration.host"]);
    });

    it("detects multiple field changes", () => {
      const oldConfig = {
        host: "localhost",
        port: 5432,
        database: "testdb",
      };

      const newConfig = {
        host: "newhost",
        port: 3306,
        database: "testdb",
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toContain("connectionConfiguration.host");
      expect(result).toContain("connectionConfiguration.port");
      expect(result).not.toContain("connectionConfiguration.database");
      expect(result).toHaveLength(2);
    });

    it("detects field addition", () => {
      const oldConfig = {
        host: "localhost",
      };

      const newConfig = {
        host: "localhost",
        port: 5432,
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual(["connectionConfiguration.port"]);
    });

    it("detects field removal", () => {
      const oldConfig = {
        host: "localhost",
        port: 5432,
      };

      const newConfig = {
        host: "localhost",
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual(["connectionConfiguration.port"]);
    });
  });

  describe("nested object changes", () => {
    it("detects changes in nested objects", () => {
      const oldConfig = {
        auth: {
          username: "user",
          password: "old_pass",
        },
      };

      const newConfig = {
        auth: {
          username: "user",
          password: "new_pass",
        },
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual(["connectionConfiguration.auth.password"]);
    });

    it("detects multiple nested changes", () => {
      const oldConfig = {
        tunnel_method: {
          tunnel_host: "old-tunnel.com",
          tunnel_port: 22,
          tunnel_user: "old_user",
          tunnel_user_password: "old_pass",
        },
      };

      const newConfig = {
        tunnel_method: {
          tunnel_host: "new-tunnel.com",
          tunnel_port: 22,
          tunnel_user: "new_user",
          tunnel_user_password: "old_pass",
        },
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toContain("connectionConfiguration.tunnel_method.tunnel_host");
      expect(result).toContain("connectionConfiguration.tunnel_method.tunnel_user");
      expect(result).not.toContain("connectionConfiguration.tunnel_method.tunnel_port");
      expect(result).not.toContain("connectionConfiguration.tunnel_method.tunnel_user_password");
      expect(result).toHaveLength(2);
    });

    it("detects deeply nested changes (4+ levels)", () => {
      const oldConfig = {
        level1: {
          level2: {
            level3: {
              level4: {
                value: "old",
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
                value: "new",
              },
            },
          },
        },
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual(["connectionConfiguration.level1.level2.level3.level4.value"]);
    });
  });

  describe("array changes", () => {
    it("detects array content changes", () => {
      const oldConfig = {
        tags: ["tag1", "tag2"],
      };

      const newConfig = {
        tags: ["tag1", "tag3"],
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual(["connectionConfiguration.tags"]);
    });

    it("detects array length changes", () => {
      const oldConfig = {
        tags: ["tag1", "tag2"],
      };

      const newConfig = {
        tags: ["tag1", "tag2", "tag3"],
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual(["connectionConfiguration.tags"]);
    });

    it("does not detect when arrays are identical", () => {
      const oldConfig = {
        tags: ["tag1", "tag2"],
      };

      const newConfig = {
        tags: ["tag1", "tag2"],
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual([]);
    });
  });

  describe("null and undefined values", () => {
    it("detects change from null to value", () => {
      const oldConfig = {
        optional: null,
      };

      const newConfig = {
        optional: "value",
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual(["connectionConfiguration.optional"]);
    });

    it("detects change from value to null", () => {
      const oldConfig = {
        optional: "value",
      };

      const newConfig = {
        optional: null,
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual(["connectionConfiguration.optional"]);
    });

    it("detects change from undefined to value", () => {
      const oldConfig = {
        optional: undefined,
      };

      const newConfig = {
        optional: "value",
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual(["connectionConfiguration.optional"]);
    });

    it("does not detect when both are null", () => {
      const oldConfig = {
        optional: null,
      };

      const newConfig = {
        optional: null,
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual([]);
    });
  });

  describe("primitive value changes", () => {
    it("detects string changes", () => {
      const oldConfig = { name: "old" };
      const newConfig = { name: "new" };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual(["connectionConfiguration.name"]);
    });

    it("detects number changes", () => {
      const oldConfig = { port: 5432 };
      const newConfig = { port: 3306 };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual(["connectionConfiguration.port"]);
    });

    it("detects boolean changes", () => {
      const oldConfig = { ssl_enabled: false };
      const newConfig = { ssl_enabled: true };

      const result = findChangedFieldPaths(oldConfig, newConfig);
      expect(result).toEqual(["connectionConfiguration.ssl_enabled"]);
    });
  });

  describe("custom prefix", () => {
    it("uses custom prefix when provided", () => {
      const oldConfig = { host: "old" };
      const newConfig = { host: "new" };

      const result = findChangedFieldPaths(oldConfig, newConfig, "custom.prefix");
      expect(result).toEqual(["custom.prefix.host"]);
    });
  });

  describe("complex real-world scenario", () => {
    it("handles Postgres connector with multiple changes", () => {
      const oldConfig = {
        host: "localhost",
        port: 5432,
        database: "testdb",
        username: "testuser",
        password: "old_password",
        tunnel_method: {
          tunnel_method: "SSH_PASSWORD_AUTH",
          tunnel_host: "tunnel.example.com",
          tunnel_port: 22,
          tunnel_user: "tunnel_user",
          tunnel_user_password: "old_tunnel_password",
        },
      };

      const newConfig = {
        host: "localhost",
        port: 5432,
        database: "testdb",
        username: "testuser",
        password: "new_password",
        tunnel_method: {
          tunnel_method: "SSH_PASSWORD_AUTH",
          tunnel_host: "new-tunnel.example.com",
          tunnel_port: 22,
          tunnel_user: "tunnel_user",
          tunnel_user_password: "new_tunnel_password",
        },
      };

      const result = findChangedFieldPaths(oldConfig, newConfig);

      expect(result).toContain("connectionConfiguration.password");
      expect(result).toContain("connectionConfiguration.tunnel_method.tunnel_host");
      expect(result).toContain("connectionConfiguration.tunnel_method.tunnel_user_password");

      expect(result).not.toContain("connectionConfiguration.host");
      expect(result).not.toContain("connectionConfiguration.port");
      expect(result).not.toContain("connectionConfiguration.database");
      expect(result).not.toContain("connectionConfiguration.username");
      expect(result).not.toContain("connectionConfiguration.tunnel_method.tunnel_method");
      expect(result).not.toContain("connectionConfiguration.tunnel_method.tunnel_port");
      expect(result).not.toContain("connectionConfiguration.tunnel_method.tunnel_user");

      expect(result).toHaveLength(3);
    });
  });
});

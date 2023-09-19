module.exports = {
  create(context) {
    return {
      MemberExpression(node) {
        if (node.object.name === "localStorage") {
          context.report({
            node,
            message: "Use the type-safe useLocalStorage hook instead of the global localStorage object",
          });
        }
      },
    };
  },
};

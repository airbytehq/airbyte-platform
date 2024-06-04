module.exports = {
  create(context) {
    return {
      ImportDeclaration(node) {
        if (
          // Check for .module.css or .module.scss imports ...
          (node.source.value.endsWith(".module.css") || node.source.value.endsWith(".module.scss")) &&
          // ... that are not assigned to any variable.
          !node.specifiers.length
        ) {
          context.report({
            node,
            message:
              'You always need to use the exported value of a (S)CSS module import e.g. `import styles from "./foobar.module.scss"`. If you only want to import global styles, remove the ".module" of the filename.',
          });
        }
      },
    };
  },
};

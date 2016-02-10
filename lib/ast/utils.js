module.exports = {
    isNameField(node, nameField) {
        return node.type === 'UnaryExpression' && node.operator === '*' && node.value === nameField;
    },

    isTime(node) {
        return node.type === 'UnaryExpression' && node.operator === '*' && node.value === 'time';
    }
};

module.exports = {
    isNameField: function(node, nameField) {
        return node.type === 'UnaryExpression' && node.operator === '*' && node.value === nameField;
    },

    isTime: function(node) {
        return node.type === 'UnaryExpression' && node.operator === '*' && node.value === 'time';
    }
};

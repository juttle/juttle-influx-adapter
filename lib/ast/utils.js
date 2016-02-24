module.exports = {
    isNameField(node, nameField) {
        return node.type === 'Field' && node.name === nameField;
    },

    isTime(node) {
        return node.type === 'Field' && node.name === 'time';
    }
};

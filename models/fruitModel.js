const mongoose = require('mongoose');

const fruitSchema = new mongoose.Schema({
    name: String,
    quantity: String,
    prix:String
});

const Fruit = mongoose.model('Fruit', fruitSchema);

module.exports = Fruit;

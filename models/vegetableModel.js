const mongoose = require('mongoose');

const vegetableSchema = new mongoose.Schema({
    name: String,
    quantity: String,
    prix:String
});

const Vegetable = mongoose.model('Vegetable', vegetableSchema);

module.exports = Vegetable;

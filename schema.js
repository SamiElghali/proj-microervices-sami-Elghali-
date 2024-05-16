const { gql } = require('@apollo/server');

// Define the GraphQL schema
const typeDefs = `#graphql
    type Fruit {
        id: String!
        name: String!
        quantity: String!
        prix:String!
    }

    type Vegetable {
        id: String!
        name: String!
        quantity: String!
        prix:String!
    }

    type Query {
        fruit(id: String!): Fruit
        fruits: [Fruit]
        vegetable(id: String!): Vegetable
        vegetables: [Vegetable]
        addFruit(name: String!, quantity: String!,prix:String!): Fruit
        addVegetable(name: String!, quantity: String!,prix:String!): Vegetable
        deleteFruit(id:String!):Fruit
        updateFruit(id:String!,name:String!,quantity:String!,prix:String!):Fruit
        deleteVegetable(id:String!):Vegetable
        updateVegetable(id:String!,name:String!,quantity:String!,prix:String!):Vegetable
    }
`;

module.exports = typeDefs;

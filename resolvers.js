const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

// Load proto files for fruits and vegetables
const fruitProtoPath = 'fruit.proto';
const vegetableProtoPath = 'vegetable.proto';

const fruitProtoDefinition = protoLoader.loadSync(fruitProtoPath, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const vegetableProtoDefinition = protoLoader.loadSync(vegetableProtoPath, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const fruitProto = grpc.loadPackageDefinition(fruitProtoDefinition).fruit;
const vegetableProto = grpc.loadPackageDefinition(vegetableProtoDefinition).vegetable;

// Define resolvers for GraphQL queries
const resolvers = {
    Query: {
        fruit: (_, { id }) => {
            // Make gRPC call to the fruit microservice
            const client = new fruitProto.FruitService('localhost:50051', grpc.credentials.createInsecure());
            return new Promise((resolve, reject) => {
                client.getFruit({ fruit_id: id }, (err, response) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(response.fruit);
                    }
                });
            });
        },
        addFruit: (_, { name, quantity,prix }) => {
            // Make gRPC call to the fruit microservice
            const client = new fruitProto.FruitService('localhost:50051', grpc.credentials.createInsecure());
            return new Promise((resolve, reject) => {
                client.addFruit({ name, quantity,prix }, (err, response) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(response.fruit);
                    }
                });
            });
        },
        updateFruit: (_, { id,name, quantity,prix }) => {
            // Make gRPC call to the fruit microservice
            const client = new fruitProto.FruitService('localhost:50051', grpc.credentials.createInsecure());
            return new Promise((resolve, reject) => {
                client.updateFruit({ id,name, quantity,prix }, (err, response) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(response.fruit);
                    }
                });
            });
        },
        deleteFruit: (_, { id }) => {
            // Make gRPC call to the fruit microservice
            const client = new fruitProto.FruitService('localhost:50051', grpc.credentials.createInsecure());
            return new Promise((resolve, reject) => {
                client.deleteFruit({ id }, (err, response) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(response.fruit);
                    }
                });
            });
        },
        fruits: () => {
            // Make gRPC call to the fruit microservice
            const client = new fruitProto.FruitService('localhost:50051', grpc.credentials.createInsecure());
            return new Promise((resolve, reject) => {
                client.searchFruits({}, (err, response) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(response.fruits);
                    }
                });
            });
        },
        vegetable: (_, { id }) => {
            // Make gRPC call to the vegetable microservice
            const client = new vegetableProto.VegetableService('localhost:50052', grpc.credentials.createInsecure());
            return new Promise((resolve, reject) => {
                client.getVegetable({ vegetable_id: id }, (err, response) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(response.vegetable);
                    }
                });
            });
        },
        addVegetable: (_, { name, quantity,prix }) => {
            // Make gRPC call to the vegetable microservice
            const client = new vegetableProto.VegetableService('localhost:50052', grpc.credentials.createInsecure());
            return new Promise((resolve, reject) => {
                client.addVegetable({ name, quantity,prix }, (err, response) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(response.vegetable);
                    }
                });
            });
        },
        updateVegetable: (_, { id,name, quantity,prix }) => {
            // Make gRPC call to the vegetable microservice
            const client = new vegetableProto.VegetableService('localhost:50052', grpc.credentials.createInsecure());
            return new Promise((resolve, reject) => {
                client.updateVegetable({ id,name, quantity,prix }, (err, response) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(response.vegetable);
                    }
                });
            });
        },
        deleteVegetable: (_, { id }) => {
            // Make gRPC call to the vegetable microservice
            const client = new vegetableProto.VegetableService('localhost:50052', grpc.credentials.createInsecure());
            return new Promise((resolve, reject) => {
                client.deleteVegetable({ id }, (err, response) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(response.vegetable);
                    }
                });
            });
        },
        vegetables: () => {
            // Make gRPC call to the vegetable microservice
            const client = new vegetableProto.VegetableService('localhost:50052', grpc.credentials.createInsecure());
            return new Promise((resolve, reject) => {
                client.searchVegetables({}, (err, response) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(response.vegetables);
                    }
                });
            });
        },
    },
};

module.exports = resolvers;

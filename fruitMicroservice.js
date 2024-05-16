const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const mongoose = require('mongoose');
const Fruit = require('./models/fruitModel');
const { Kafka } = require('kafkajs');

const fruitProtoPath = 'fruit.proto';
const fruitProtoDefinition = protoLoader.loadSync(fruitProtoPath, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});
const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();
const fruitProto = grpc.loadPackageDefinition(fruitProtoDefinition).fruit;

const url = 'mongodb://localhost:27017/fruitsDB';

mongoose.connect(url)
    .then(() => {
        console.log('Connected to the database!');
    }).catch((err) => {
        console.log(err);
    })

const consumer = kafka.consumer({ groupId: 'api-gateway-consumer' });

consumer.subscribe({ topic: 'vegetable_topic' });

(async () => {
    await consumer.connect();
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Received message: ${message.value.toString()}, from topic: ${topic}`);
        },
    });
})();
const fruitService = {
    getFruit: async (call, callback) => {
        await producer.connect();
        try {
            const fruitId = call.request.fruit_id;
            const fruit = await Fruit.findOne({ _id: fruitId }).exec();
            await producer.send({
                topic: 'fruit_topic',
                messages: [{ value: 'Searched for fruit id: ' + fruitId.toString() }],
            });
            if (!fruit) {
                callback({ code: grpc.status.NOT_FOUND, message: 'Fruit not found' });
                return;
            }
            callback(null, { fruit });
        } catch (error) {
            await producer.send({
                topic: 'fruit_topic',
                messages: [{ value: `Error occurred while fetching fruit: ${error}` }],
            });
            callback({ code: grpc.status.INTERNAL, message: 'Error occurred while fetching fruit' });
        }
    },
    deleteFruit: async (call, callback) => {
        const { id } = call.request;
        console.log("Logging from gRPC!");
        await producer.connect();
        try {
            // Trouver le fruit par son identifiant et le supprimer
            const deletedFruit = await Fruit.findByIdAndDelete(id);
            if (!deletedFruit) {
                callback({ code: grpc.status.NOT_FOUND, message: 'Fruit not found' });
                return;
            }
            await producer.send({
                topic: 'fruit_topic',
                messages: [{ value: 'Deleted fruit' }],
            });
            callback(null, { fruit: deletedFruit });
        } catch (error) {
            console.error('Error occurred while deleting Fruit:', error);
            callback({ code: grpc.status.INTERNAL, message: 'Error occurred while deleting Fruit' });
        }
    },
    // Méthode gRPC pour mettre à jour un fruit
    updateFruit: async (call, callback) => {
        console.log("Logging from gRPC!");
        const { id, name, quantity, prix } = call.request;
        const _id = id;
        await producer.connect();
        try {
            // Trouver le fruit par son identifiant et mettre à jour ses champs
            const updatedFruit = await Fruit.findByIdAndUpdate(_id, { name, quantity, prix }, { new: true });
            if (!updatedFruit) {
                callback({ code: grpc.status.NOT_FOUND, message: 'Fruit not found' });
                return;
            }
            await producer.send({
                topic: 'fruit_topic',
                messages: [{ value: 'Updated fruit' }],
            });
            callback(null, { fruit: updatedFruit });
        } catch (error) {
            console.error('Error occurred while updating Fruit:', error);
            callback({ code: grpc.status.INTERNAL, message: 'Error occurred while updating Fruit' });
        }
    },
    searchFruits: async (call, callback) => {
        try {
            const fruits = await Fruit.find({}).exec();
            await producer.connect();
            await producer.send({
                topic: 'fruit_topic',
                messages: [{ value: 'Searched for fruits' }],
            });
            callback(null, { fruits });
        } catch (error) {
            await producer.send({
                topic: 'fruit_topic',
                messages: [{ value: `Error occurred while fetching fruits: ${error}` }],
            });
            callback({ code: grpc.status.INTERNAL, message: 'Error occurred while fetching fruits' });
        }
    },
    addFruit: async (call, callback) => {
        const { name, quantity,prix } = call.request;
        const newFruit = new Fruit({ name, quantity,prix });

        try {
            await producer.connect();
            await producer.send({
                topic: 'fruit_topic',
                messages: [{ value: JSON.stringify(newFruit) }],
            });
            await producer.disconnect();
            const savedFruit = await newFruit.save();
            callback(null, { fruit: savedFruit });
        } catch (error) {
            callback({ code: grpc.status.INTERNAL, message: 'Error occurred while adding fruit' });
        }
    }
};

const server = new grpc.Server();
server.addService(fruitProto.FruitService.service, fruitService);
const port = 50051;
server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(),
    (err, port) => {
        if (err) {
            console.error('Failed to bind server:', err);
            return;
        }
        console.log(`Server is running on port ${port}`);
        server.start();
    });
console.log(`Fruit microservice is running on port ${port}`);

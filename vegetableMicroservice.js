// vegetableMicroservice.js
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
// Load the vegetable.proto file
const vegetableProtoPath = 'vegetable.proto';
const mongoose = require('mongoose');
const Vegetables = require('./models/vegetableModel');
const { Kafka } = require('kafkajs');

const vegetableProtoDefinition = protoLoader.loadSync(vegetableProtoPath, {
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

const vegetableProto = grpc.loadPackageDefinition(vegetableProtoDefinition).vegetable;
const url = 'mongodb://localhost:27017/vegetables';

mongoose.connect(url)
    .then(() => {
        console.log('connected to database!');
    }).catch((err) => {
        console.log(err);
    })
    const consumer = kafka.consumer({ groupId: 'api-gateway-consumer' });

    consumer.subscribe({ topic: 'fruit_topic' });
    
    (async () => {
        await consumer.connect();
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log(`Received message: ${message.value.toString()}, from topic: ${topic}`);
            },
        });
    })();
const vegetableService = {
    getVegetable: async (call, callback) => {
        try {
            const vegetableId = call.request.vegetable_id;
            const vegetable = await Vegetables.findOne({ _id: vegetableId }).exec();
            await producer.connect();
            await producer.send({
                topic: 'vegetable_topic',
                messages: [{ value: 'Searched for vegetable id: ' + vegetableId.toString() }],
            });

            if (!vegetable) {
                callback({ code: grpc.status.NOT_FOUND, message: 'Vegetable not found' });
                return;
            }
            callback(null, { vegetable });
        } catch (error) {
            await producer.connect();
            await producer.send({
                topic: 'vegetable_topic',
                messages: [{ value: `Error occurred while fetching vegetables: ${error}` }],
            });
            callback({ code: grpc.status.INTERNAL, message: 'Error occurred while fetching vegetable' });
        }
    },
    deleteVegetable: async (call, callback) => {
        const { id } = call.request;
        console.log("Logging from gRPC!");
        try {
            // Trouver le légume par son identifiant et le supprimer
            const deletedVegetable = await Vegetables.findByIdAndDelete(id);
            if (!deletedVegetable) {
                callback({ code: grpc.status.NOT_FOUND, message: 'Vegetable not found' });
                return;
            }
            await producer.connect();
            await producer.send({
                topic: 'vegetable_topic',
                messages: [{ value: 'Deleated veg ' }],
            });

            callback(null, { vegetable: deletedVegetable });
        } catch (error) {
            console.error('Error occurred while deleting Vegetable:', error);
            callback({ code: grpc.status.INTERNAL, message: 'Error occurred while deleting Vegetable' });
        }
    },
    // Méthode gRPC pour mettre à jour un légume
    updateVegetable: async (call, callback) => {
        console.log("Logging from gRPC!");
        const { id, name, quantity, prix } = call.request;
        const _id = id;
        try {
            // Trouver le légume par son identifiant et mettre à jour ses champs
            const updatedVegetable = await Vegetables.findByIdAndUpdate(_id, { name, quantity, prix }, { new: true });
            if (!updatedVegetable) {
                callback({ code: grpc.status.NOT_FOUND, message: 'Vegetable not found' });
                return;
            }
            await producer.connect();
            await producer.send({
                topic: 'vegetable_topic',
                messages: [{ value: 'Updated veg ' }],
            });
            callback(null, { vegetable: updatedVegetable });
        } catch (error) {
            console.error('Error occurred while updating Vegetable:', error);
            callback({ code: grpc.status.INTERNAL, message: 'Error occurred while updating Vegetable' });
        }
    },
    searchVegetables: async (call, callback) => {
        try {
            const vegetables = await Vegetables.find({}).exec();

           await producer.connect();
            await producer.send({
                topic: 'vegetable_topic',
                messages: [{ value: 'Searched for vegetables' }],
            });

            callback(null, { vegetables });
        } catch (error) {
            await producer.connect();
            await producer.send({
                topic: 'vegetable_topic',
                messages: [{ value: `Error occurred while fetching vegetables: ${error}` }],
            });

            callback({ code: grpc.status.INTERNAL, message: 'Error occurred while fetching vegetables' });
        }
    },
    addVegetable: async (call, callback) => {
        const { name, quantity,prix } = call.request;
        const newVegetable = new Vegetables({ name, quantity,prix });

        try {
           await producer.connect();

            await producer.send({
                topic: 'vegetable_topic',
                messages: [{ value: JSON.stringify(newVegetable) }],
            });

            await producer.disconnect();

            const savedVegetable = await newVegetable.save();

            callback(null, { vegetable: savedVegetable });
        } catch (error) {
            callback({ code: grpc.status.INTERNAL, message: 'Error occurred while adding vegetable' });
        }
    }
};

// Create and start the gRPC server
const server = new grpc.Server();
server.addService(vegetableProto.VegetableService.service, vegetableService);
const port = 50052;
server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(),
    (err, port) => {
        if (err) {
            console.error('Failed to bind server:', err);
            return;
        }
        console.log(`Server is running on port ${port}`);
        server.start();
    });
console.log(`Vegetable microservice running on port ${port}`);

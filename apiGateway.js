const express = require('express');
const { ApolloServer } = require('@apollo/server');
const { expressMiddleware } = require('@apollo/server/express4');
const bodyParser = require('body-parser');
const cors = require('cors');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Kafka } = require('kafkajs');

// Load proto files for fruits and vegetables
const fruitProtoPath = 'fruit.proto';
const vegetableProtoPath = 'vegetable.proto';
const resolvers = require('./resolvers');
const typeDefs = require('./schema');

// Create a new Express application
const app = express();
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
app.use(bodyParser.json());
const fruitProto = grpc.loadPackageDefinition(fruitProtoDefinition).fruit;
const vegetableProto = grpc.loadPackageDefinition(vegetableProtoDefinition).vegetable;

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092'] 
});

const consumer = kafka.consumer({ groupId: 'api-gateway-consumer' });

consumer.subscribe({ topic: 'fruit_topic' });
consumer.subscribe({ topic: 'vegetable_topic' });

(async () => {
    await consumer.connect();
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Received message: ${message.value.toString()}, from topic: ${topic}`);
        },
    });
})();

// Create ApolloServer instance with imported schema and resolvers
const server = new ApolloServer({ typeDefs, resolvers });

// Apply ApolloServer middleware to Express application
server.start().then(() => {
    app.use(
        cors(),
        bodyParser.json(),
        expressMiddleware(server),
    );
});

app.get('/fruits', (req, res) => {
    const client = new fruitProto.FruitService('localhost:50051',
        grpc.credentials.createInsecure());
    client.searchFruits({}, (err, response) => {
        if (err) {
            res.status(500).send(err);
        } else {
            res.json(response.fruits);
        }
    });
});
//update fruit 
app.put('/fruits/:id', (req, res) => {
    const client = new fruitProto.FruitService('localhost:50051',
        grpc.credentials.createInsecure());
    const id = req.params.id;
    const data=req.body;
    const name = data.name;
    const quantity = data.quantity;
    const prix=data.prix;
    client.updateFruit({ id,name,quantity,prix }, (err, response) => {
        if (err) {
            res.status(500).send(err);
        } else {
            res.json(response.fruit);
        }
    });
});
app.delete('/fruits/:id', (req, res) => {
    const client = new fruitProto.FruitService('localhost:50051',
        grpc.credentials.createInsecure());
    const id = req.params.id;

    client.deleteFruit({ id }, (err, response) => {
        if (err) {
            res.status(500).send(err);
        } else {
            res.json(response.fruit);
        }
    });
});
app.get('/fruits/:id', (req, res) => {
    const client = new fruitProto.FruitService('localhost:50051',
        grpc.credentials.createInsecure());
    const id = req.params.id;
    client.getFruit({ id }, (err, response) => {
        if (err) {
            res.status(500).send(err);
        } else {
            res.json(response.fruit);
        }
    });
});

app.post('/fruits/add', (req, res) => {
    const client = new fruitProto.FruitService('localhost:50051',
        grpc.credentials.createInsecure());
    const data = req.body;
    const name = data.name;
    const quantity = data.quantity;
    const prix=data.prix;
    client.addFruit({ name, quantity,prix }, (err, response) => {
        if (err) {
            res.status(500).send(err);
        } else {
            res.json(response.fruit);
        }
    });
});

app.get('/vegetables', (req, res) => {
    const client = new vegetableProto.VegetableService('localhost:50052',
        grpc.credentials.createInsecure());
    client.searchVegetables({}, (err, response) => {
        if (err) {
            res.status(500).send(err);
        } else {
            res.json(response.vegetables);
        }
    });
});

app.get('/vegetables/:id', (req, res) => {
    const client = new vegetableProto.VegetableService('localhost:50052',
        grpc.credentials.createInsecure());
    const id = req.params.id;
    client.getVegetable({ vegetable_id: id }, (err, response) => {
        if (err) {
            res.status(500).send(err);
        } else {
            res.json(response.vegetable);
        }
    });
});
//update fruit 
app.put('/vegetables/:id', (req, res) => {
    const client = new vegetableProto.VegetableService('localhost:50052',
        grpc.credentials.createInsecure());
    const id = req.params.id;
    const data=req.body;
    const name = data.name;
    const quantity = data.quantity;
    const prix=data.prix;
    client.updateVegetable({ id,name,quantity,prix }, (err, response) => {
        if (err) {
            res.status(500).send(err);
        } else {
            res.json(response.vegetable);
        }
    });
});
app.delete('/vegetables/:id', (req, res) => {
    const client = new vegetableProto.VegetableService('localhost:50052',
        grpc.credentials.createInsecure());
    const id = req.params.id;

    client.deleteVegetable({ id }, (err, response) => {
        if (err) {
            res.status(500).send(err);
        } else {
            res.json(response.vegetable);
        }
    });
});
app.post('/vegetables/add', (req, res) => {
    const client = new vegetableProto.VegetableService('localhost:50052',
        grpc.credentials.createInsecure());
    const data = req.body;
    const name = data.name;
    const quantity = data.quantity;
    const prix =data.prix;
    client.addVegetable({ name, quantity,prix }, (err, response) => {
        if (err) {
            res.status(500).send(err);
        } else {
            res.json(response.vegetable);
        }
    });
});

// Start Express application
const port = 3000;
app.listen(port, () => {
    console.log(`API Gateway is running on port ${port}`);
});

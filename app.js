const express = require('express');
const EventSource = require('eventsource');
const mqtt = require('mqtt');

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(require('cors')());

const mqttBrokerAddress = 'hikar.cloud';
const mqttBrokerPort = 1883;

const mqttClient = mqtt.connect(`mqtt://${mqttBrokerAddress}:${mqttBrokerPort}`);

const sseQueues = {};
const resultsQueues = {};

mqttClient.on('connect', () => {
    console.log(`Connected to MQTT broker`);

    const topic = 'Hikar/DeviceUID/PHA8741D0850FD/#';
    mqttClient.subscribe(topic);
});

mqttClient.on('message', (topic, message) => {
    const mqttData = { topic, message: message.toString() };
    for (const queueKey in sseQueues) {
        if (mqttData.message.includes(queueKey)) {
            sseQueues[queueKey].push(mqttData);
        }
    }
});

function processMqttData(key) {
    setInterval(() => {
        if (!(key in sseQueues)) {
            sseQueues[key] = [];
        }

        const sseQueue = sseQueues[key];
        if (sseQueue.length === 0) return;

        const mqttData = sseQueue.shift();
        const message = mqttData.message;
        const objects = message.split('{').slice(1);
        const formattedObjects = objects.map((obj) => (obj.startsWith('"') ? '{' + obj : obj));
        const jsonData = '[' + formattedObjects.join(',') + ']';

        const parsedData = JSON.parse(jsonData);
        const f5HeatOntValue = parsedData.find((obj) => key in obj)?.[key] || null;

        if (f5HeatOntValue !== null) {
            console.log(`Value of ${key}: ${f5HeatOntValue}`);
            if (!(key in resultsQueues)) {
                resultsQueues[key] = [];
            }
            resultsQueues[key].push(f5HeatOntValue);
        }
    }, 100);
}

app.get('/mqtt-data-stream/:key', (req, res) => {
    const { key } = req.params;
    processMqttData(key);
    sendSseData(res, key);
});

function sendSseData(res, key) {
    res.set({
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*',
    });

    setInterval(() => {
        if (!(key in resultsQueues)) {
            resultsQueues[key] = [];
        }

        const resultsQueue = resultsQueues[key];
        if (resultsQueue.length === 0) return;

        const f5HeatOntValue = resultsQueue.shift();
        res.write(`data: ${f5HeatOntValue}\n\n`);
    }, 100);
}

const server = app.listen(5002, () => {
    console.log('Server running at http://localhost:5002/');
});

process.on('SIGINT', () => {
    mqttClient.end();
    server.close(() => {
        process.exit(0);
    });
});

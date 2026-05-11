const mqtt = require('mqtt');
const fetch = require('node-fetch');

// 1. Configuration (Use Environment Variables on Railway!)
const LICHESS_TOKEN = process.env.LICHESS_TOKEN;
const MQTT_URL = process.env.MQTT_URL; // e.g., tls://xxxxxx.s1.eu.hivemq.cloud:8883
const MQTT_USER = process.env.MQTT_USER;
const MQTT_PASS = process.env.MQTT_PASS;
const TOPIC = 'chess/status';

// 2. Setup MQTT Client
const client = mqtt.connect(MQTT_URL, {
    username: MQTT_USER,
    password: MQTT_PASS,
    rejectUnauthorized: false // Required for HiveMQ Cloud TLS
});

client.on('connect', () => {
    console.log('✅ Connected to HiveMQ Cloud');
    startLichessStream();
});

// 3. Stream from Lichess
async function startLichessStream() {
    console.log('♟️ Starting Lichess stream...');
    
    // This example streams events for the authenticated user
    const response = await fetch('https://lichess.org/api/stream/event', {
        headers: { 'Authorization': `Bearer ${LICHESS_TOKEN}` }
    });

    // Read the stream line by line
    const reader = response.body;
    reader.on('data', (chunk) => {
        const data = chunk.toString();
        if (data.trim()) {
            try {
                const event = JSON.parse(data);
                console.log('New Event:', event.type);

                // Send to ESP32 via MQTT
                // You can send the whole JSON or just a specific status like event.game.id
                client.publish(TOPIC, JSON.stringify({
                    type: event.type,
                    id: event.game?.id || 'none',
                    status: event.game?.status?.name || 'active'
                }));
            } catch (e) {
                // Ignore heartbeats (empty lines)
            }
        }
    });

    reader.on('error', (err) => {
        console.error('Stream Error:', err);
        setTimeout(startLichessStream, 5000); // Reconnect on error
    });
}

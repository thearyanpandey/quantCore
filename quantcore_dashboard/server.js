const express = require('express');
const http = require('http');
const {Server} = require("socket.io");
const {createClient} = require('redis');
const {Pool} = require('pg');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.static('public'));    //Serve the HTML file

const server = http.createServer(app);
const io = new Server(server, {cors: {origin: "*"}});

// DB Connection 
const pool = new Pool({
    user: 'admin',
    host: 'localhost',
    database: 'quantcore',
    password: 'secret',
    port: 5432,
});

//Redis Connection
const redisSubscriber = createClient({url: 'redis://localhost:6379'});

redisSubscriber.on('error', (err) => console.log('Redis Client Error', err));

(async() => {
    await redisSubscriber.connect();

})();

const redisClient = createClient({url: 'redis://localhost:6379'});
redisClient.connect();

setInterval(async () => {
    try {
        const btcStream = await redisClient.xRevRange('market_data:BTCUSDT', '+', '-', {COUNT:1});
        if (btcStream && btcStream.length > 0){
            const data = JSON.parse(btcStream[0].message.data);
            io.emit('market_tick', data);
        }

        const signals = await redisClient.xRevRange('trade_signals', '+', '-', {COUNT: 1});
        if (signals && signals.length > 0){
            const data = JSON.parse(signals[0].message.json);
            io.emit('trade_signal', data);
        }
    } catch (error) {
        console.error("Polling error: ", e);
    }
}, 500)

app.get('/api/histroy', async(req, res) => {
    try {
        const result = await pool.query('SELECT * FROM trades ORDER BY timestamp DESC LIMIT 50');
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({error: err.message});
    }
});

server.listen(3000, () => {
    console.log('Dashboard running at https://localhost:3000');
});
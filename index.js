const express = require('express');
const axios = require('axios');
const compression = require('compression');
const http = require('http'); 
const WebSocket = require('ws'); 
const zlib = require('zlib'); 
const app = express();

// --- RENDER MODIFICATION ---
const PORT = process.env.PORT || 8080; 
// ---------------------------

app.use(compression());

app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*"); 
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

const KATAULA_TIME = 3000; 
let cachedData = { status: "success", data: [] }; 
let jumpTracker = {}; 
let globalLatestTimeStr = "00:00:00"; 
let currentIndex = 0; 

let lastBroadcastedDataMap = new Map(); 

const JUMP_THRESHOLD = 0.30; 
const STABILITY_COUNT_REQUIRED = 8; 

const allowedSymbols = ["MGLD-BA", "GLMM", "GOLDGUINEA", "GOLDPETAL", "MSIL-BA", "SLMM-BA", "SILMIC-BA", "MCRO-BA", "CRUDEOILM", "MNAG-BA", "NATGASMINI", "MALU", "ALUMINI", "MCOP-BA", "MLEA", "LEADMINI", "MZIN", "ZINCMINI", "ELECDMBL", "MMENO", "MUSDINR", "MGLD#", "GLMM#", "GOLDGUINEA#", "GOLDPETAL#", "MSIL#", "SLMM#", "SILMIC#", "MCRO#", "CRUDEOILM#", "MNAG#", "MALU#", "MCOP#", "MLEA#", "MZIN#"];

let gasUrls = [
    { id: 0, url: 'https://script.google.com/macros/s/AKfycbw7-ixJDSBxQqV3wkXpgwzvihf2l8GEnIcBbBBsK_2Y-LZ9bkl23jfZ7fSPMuuOC9IW/exec' },
    { id: 1, url: 'https://script.google.com/macros/s/AKfycby5v2mhUVkNDaZJqQDiDk4J4pSTtB_q-cuNjtSv_sKO5zoxruCppTUE5E16m0cm9Ks/exec' },
    { id: 2, url: 'https://script.google.com/macros/s/AKfycbx6aa9gTJMnTTe5r4i0xIkOtzgREd17uQNafSP4H3et32eYbg_70ED0-BCuWouRXfeXYw/exec' },
    { id: 3, url: 'https://script.google.com/macros/s/AKfycbw9QQGDJuJwngIlS3Mh6COUyGN7SSWOvH1tIDlANRYMmxlqtsiNRZTM2ZozvxgoUfIivg/exec' },
    { id: 4, url: 'https://script.google.com/macros/s/AKfycby0Ms3XlcV7_cRXJDZwOEa7dJLE5fO6tmdJZJ3e5nWlncd9bSOAoRlhSNFPHm8r2E8/exec' },
    { id: 5, url: 'https://script.google.com/macros/s/AKfycbwEOrbAt_5MNnkCPC5-Zd06nWAhmS5ROUSItxFgTgeQ4aY5nGHgRxBYgc5Llubxhh9Zwg/exec' },
    { id: 6, url: 'https://script.google.com/macros/s/AKfycbwC1fS5OUgflJI__O4t3mVYpnC631onCyoKoA7pT1u3g_sxtW5-v0nt21tUFY4hC6Gb/exec' },
    { id: 7, url: 'https://script.google.com/macros/s/AKfycbwMwXt9PxWG5CZBqp4TPIMM4TPdsfHde3hwwFUs4NuLLu_GKdq6K3v1ICI4LJrwMs8d/exec' }
];

function isInvalid(val) {
    return val === null || val === undefined || val === "" || val === "0" || val === "0.00" || val === "0000" || val === "----" || isNaN(parseFloat(val));
}

function getISTTime() {
    return new Date(new Date().toLocaleString("en-US", {timeZone: "Asia/Kolkata"}));
}

async function fetchFromGas(gasObj) {
    const res = await axios.get(gasObj.url, { timeout: 12000 });
    if (res.data && Array.isArray(res.data.data)) return res.data;
    throw new Error("Invalid Format");
}

function broadcastData(newData) {
    const deltaData = newData.data.filter(newItem => {
        const oldItem = lastBroadcastedDataMap.get(newItem.id);
        if (!oldItem) return true;

        const hasChanged = 
            oldItem.ltp !== newItem.ltp ||
            oldItem.bid !== newItem.bid ||
            oldItem.ask !== newItem.ask ||
            oldItem.high !== newItem.high ||
            oldItem.low !== newItem.low ||
            oldItem.close !== newItem.close ||
            oldItem.time !== newItem.time;

        return hasChanged || newItem.id === "TIME"; 
    });

    if (deltaData.length === 0) return;

    newData.data.forEach(item => {
        lastBroadcastedDataMap.set(item.id, { ...item });
    });

    const jsonString = JSON.stringify({ status: "success", data: deltaData });
    zlib.deflate(jsonString, { level: 1 }, (err, buffer) => {
        if (!err) {
            wss.clients.forEach((client) => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(buffer, { binary: true, compress: false });
                }
            });
        }
    });
}

async function updateCacheInBackground() {
    const nowIST = getISTTime();
    const currentTimestamp = Date.now();
    
    const currentHour = nowIST.getHours();
    const currentMin = nowIST.getMinutes();
    const currentSec = nowIST.getSeconds();

    const isMorningOpening = (currentHour === 9 && (currentMin === 0 || (currentMin === 1 && currentSec <= 30)));

    const selectedUrl = gasUrls[currentIndex];
    currentIndex = (currentIndex + 1) % gasUrls.length;

    try {
        const newData = await fetchFromGas(selectedUrl);
        
        let incomingMaxTimeStr = "00:00:00";
        newData.data.forEach(d => {
            if (d.time && d.time > incomingMaxTimeStr) incomingMaxTimeStr = d.time;
        });

        // --- LATEST TIME PROTECTION ---
        if (!isMorningOpening && incomingMaxTimeStr < globalLatestTimeStr && globalLatestTimeStr !== "00:00:00") {
            return; 
        }

        const finalDataList = allowedSymbols.map(symbolId => {
            const newItem = newData.data.find(d => d.id === symbolId);
            const oldItem = (cachedData.data && cachedData.data.find(d => d.id === symbolId)) || null;

            if (!newItem) return oldItem;

            // Individual symbol time check
            if (!isMorningOpening && oldItem && newItem.time && newItem.time < oldItem.time) {
                return oldItem;
            }

            if (isMorningOpening) {
                delete jumpTracker[symbolId];
            } else if (oldItem) {
                const nClose = parseFloat(newItem.close) || parseFloat(oldItem.close) || 0;
                if (nClose !== 0) {
                    const fields = ['ltp', 'high', 'low', 'bid', 'ask'];
                    let hasJump = false;
                    for (let field of fields) {
                        const newVal = parseFloat(newItem[field]) || 0;
                        const oldVal = parseFloat(oldItem[field]) || 0;
                        if (newVal !== 0 && oldVal !== 0) {
                            const newFieldPcnt = ((newVal - nClose) / nClose) * 100;
                            const oldFieldPcnt = ((oldVal - nClose) / nClose) * 100;
                            if (Math.abs(newFieldPcnt - oldFieldPcnt) > JUMP_THRESHOLD) {
                                hasJump = true;
                                break; 
                            }
                        }
                    }

                    if (hasJump) {
                        if (!jumpTracker[symbolId]) {
                            jumpTracker[symbolId] = { startTime: currentTimestamp, count: 1, lastLtp: parseFloat(newItem.ltp) };
                            return oldItem; 
                        } else {
                            const trk = jumpTracker[symbolId];
                            const currentLtp = parseFloat(newItem.ltp) || 0;
                            if (Math.abs(currentLtp - trk.lastLtp) < 0.10) trk.count++;
                            else { trk.count = 1; trk.lastLtp = currentLtp; }

                            if (trk.count < STABILITY_COUNT_REQUIRED && (currentTimestamp - trk.startTime) < 45000) {
                                return oldItem;
                            }
                            delete jumpTracker[symbolId];
                        }
                    } else {
                        delete jumpTracker[symbolId];
                    }
                }
            }

            let mergedItem = oldItem ? { ...oldItem } : { ...newItem };
            ['ltp', 'bid', 'ask', 'high', 'low', 'close', 'time'].forEach(field => {
                if (!isInvalid(newItem[field])) {
                    mergedItem[field] = newItem[field];
                }
            });

            const finalLtp = parseFloat(mergedItem.ltp) || 0;
            const finalClose = parseFloat(mergedItem.close) || 0;
            const finalPcnt = finalClose !== 0 ? ((finalLtp - finalClose) / finalClose) * 100 : 0;

            return {
                ...mergedItem,
                ltp: finalLtp.toFixed(2),
                pcnt_chg: finalPcnt.toFixed(2),
                bid: (parseFloat(mergedItem.bid) || 0).toFixed(2),
                ask: (parseFloat(mergedItem.ask) || 0).toFixed(2),
                high: (parseFloat(mergedItem.high) || 0).toFixed(2),
                low: (parseFloat(mergedItem.low) || 0).toFixed(2),
                close: finalClose.toFixed(2)
            };
        });

        if (incomingMaxTimeStr > globalLatestTimeStr || isMorningOpening) {
            globalLatestTimeStr = incomingMaxTimeStr;
        }

        let finalResult = finalDataList.filter(d => d !== null);
        finalResult.push({ id: "TIME", time: globalLatestTimeStr, ltp: "0.00", pcnt_chg: "0.00" });

        cachedData = { status: "success", data: finalResult };
        broadcastData(cachedData); 
        console.log(`Update: GAS ID ${selectedUrl.id} at ${globalLatestTimeStr}`);

    } catch (err) {
        console.log(`Fetch failed for GAS ID ${selectedUrl.id}`);
    }
}

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, perMessageDeflate: false });

wss.on('connection', (ws) => {
    const fullJson = JSON.stringify(cachedData);
    zlib.deflate(fullJson, { level: 1 }, (err, buffer) => {
        if (!err) ws.send(buffer, { binary: true, compress: false });
        else ws.send(fullJson);
    });
});

setInterval(updateCacheInBackground, KATAULA_TIME);
app.get('/flight-data', (req, res) => res.json(cachedData));

// --- RENDER MODIFICATION ---
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server Live on port ${PORT}`);
});

const express = require('express');
const axios = require('axios');
const compression = require('compression');
const http = require('http'); 
const WebSocket = require('ws'); 
const zlib = require('zlib'); 
const app = express();

const PORT = process.env.PORT || 8080; 
app.use(compression());

app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*"); 
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

const KATAULA_TIME = 2500; 
const BOOTSTRAP_URL = "https://api.databhavwow.top/live_v3.json"; // Server jagne par yahan se data lega
let cachedData = { status: "success", data: [] }; 
let jumpTracker = {}; 
let globalLatestTimeStr = "00:00:00"; 
let currentIndex = 0; 
let lastBroadcastedDataMap = new Map(); 

const JUMP_THRESHOLD = 0.30; 
const STABILITY_COUNT_REQUIRED = 8; 

const allowedSymbols = ["MGLD-BA", "GLMM", "GOLDGUINEA", "GOLDPETAL", "MSIL-BA", "SLMM-BA", "SILMIC-BA", "MCRO-BA", "CRUDEOILM", "MNAG-BA", "NATGASMINI", "MALU", "ALUMINI", "MCOP-BA", "MLEA", "LEADMINI", "MZIN", "ZINCMINI", "ELECDMBL", "MMENO", "MUSDINR", "MGLD#", "GLMM#", "GOLDGUINEA#", "GOLDPETAL#", "MSIL#", "SLMM#", "SILMIC#", "MCRO#", "CRUDEOILM#", "MNAG#", "MALU#", "MCOP#", "MLEA#", "MZIN#"];

let gasUrls = [
    { id: 0, url: 'https://script.googleusercontent.com/macros/echo?user_content_key=AWDtjMXCc8xM7nMyGqryR8rsaqkYgnx6it9rYvKm3OrM1sH8DPBEr0JoJ337plneez6CJIkZS12GIi7b_zbdV1x99BuA3EVPwmx05WSOe0JSOanHR_32BdocFrR-NMOLGFq46MIMMI84H76a5s_dLz7n_Me7fyq-eoaaSwRqT7Lv5oVfNHBIqDXjf_rJqw--VwhlOU0CTcghtZT9cW7pogyndjFvd6DZ3ZAay_rX8f2NfTy0U9TgmvLxuf5N6K99r-74uPW--KPXsmnoRfDstuzotKhZ-3wfMA&lib=MHsNRYD6QQ15MrNbwshqB06CjTPFH_w2u' },
    { id: 1, url: 'https://script.googleusercontent.com/macros/echo?user_content_key=AWDtjMUlbX2JrTQGmseXnfoETKaR56iDVZcN1ATi4SgH_uzQ2n5v59oFHPL5X_UBJXKmpWNK8usPP_w3cQvqc2qoYVgE0RH0RgOHKFqLnCFXC1Bfktee2P3r2WtMIN7SBS5Nw70Q84JJQsFV7bDuhNtYmasQtJSsh6pEOj9lrhtNCU0u1A7eidvF9e-v8BYx4Kt22V-8DzHoUZOeh-V3TwzBK6BOmK81MN6SoWmJ6qMvnzjzWUY0txABRTC6CA_qxRNKvW0sMeU605a0PQvI0zzC-M4bDEPb7f9DAHi3Us-m&lib=MFHdiYIrc1YCDl9lkLqJ56ltBZjiLpsLm' },
    { id: 2, url: 'https://script.googleusercontent.com/macros/echo?user_content_key=AWDtjMWR8K8F65ka_yyKo-GZbXnHKogTifX2gUql9McjUzjw8fAi1aL7tplms7fYqLFfewCQHgdpbIpfleazeX0FFarT-Ll2PXCedisbrjTeDNEv3FJ_WkAG5VhdYlmBzsFUTk4YNTu8WcK5jsx76BrP02U2iazilA2YnycytjgxCqR0g66SKrqlVbmtYHnAgvcc4h0JWUMtzLKEkLMX7i_oQmeOZIDxMIfX0il-v4UYP5p-aE20bCkHbVEWbIZiu0aHgvcxhM924n7bu1YCRhRr8xinmwTNcw&lib=M_8-CAUC7vvUvcWhR9P2i1pXv_sM7Qa-L' },
    { id: 3, url: 'https://script.googleusercontent.com/macros/echo?user_content_key=AWDtjMXDzM51KkYWpzatyVWECzoV93YoB9pkWRq9aGY9XrdRrN6VR7oTAklbQ0kXdQtGd0tKRMNAefjPHzBU_RUI3RTi2jUkw7IOU1jLR-QWZYw6czvVgVj_LbRL9fnO7ATOH7JLl8hzf_U49fBxNGgT3qJ_iOLeq9eHFk154h02WqJspnsNbJryJaPnjZt_wtn-VK0AS4fN9oJsQ4OfP_jVJ_ivpyG7f3Nf_CX09SLBaIdOGftMNSA5DIj0w4eSQV5cHbVOFPdD_etIBwpZwmNIHYqrEP5pQQ&lib=MSESn-4ATh6JSxOffRDxIpZ6PZ5UckKJC' }
];

function isInvalid(val) {
    return val === null || val === undefined || val === "" || val === "0" || val === "0.00" || val === "0000" || val === "----" || isNaN(parseFloat(val));
}

function getISTTime() {
    return new Date(new Date().toLocaleString("en-US", {timeZone: "Asia/Kolkata"}));
}

function getProcessedTime(rawTime) {
    if (!rawTime) return "23:30:00";
    if (rawTime > "23:30:00" || rawTime < "08:59:56") {
        return "23:30:00";
    }
    return rawTime;
}

// Fetch helper that can handle both GAS and Bootstrap URL
async function fetchData(url) {
    const res = await axios.get(url, { timeout: 10000 });
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
            oldItem.time !== newItem.time;
        return hasChanged || newItem.id === "TIME"; 
    });
    if (deltaData.length === 0) return;
    newData.data.forEach(item => { lastBroadcastedDataMap.set(item.id, { ...item }); });
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
    const timeNowStr = nowIST.toTimeString().split(' ')[0]; 

    // Logic: 09:00:00 se 09:01:30 tak jump-free opening
    const isMorningOpening = (timeNowStr >= "09:00:00" && timeNowStr <= "09:01:30");
    
    // Subha 9 baje se pehle API se data layega, uske baad GAS se
    const isBeforeMarket = (timeNowStr < "09:00:00" && timeNowStr > "01:00:00");
    
    let targetUrl;
    if (isBeforeMarket) {
        targetUrl = BOOTSTRAP_URL;
    } else {
        targetUrl = gasUrls[currentIndex].url;
        currentIndex = (currentIndex + 1) % gasUrls.length;
    }

    try {
        const newData = await fetchData(targetUrl);
        let incomingMaxTimeStr = "00:00:00";
        newData.data.forEach(d => {
            if (d.time && d.time > incomingMaxTimeStr) incomingMaxTimeStr = d.time;
        });

        // Skip if data is older (unless it's morning reset)
        if (!isMorningOpening && incomingMaxTimeStr < globalLatestTimeStr && globalLatestTimeStr !== "00:00:00") {
            return; 
        }

        const finalDataList = allowedSymbols.map(symbolId => {
            const newItem = newData.data.find(d => d.id === symbolId);
            const oldItem = (cachedData.data && cachedData.data.find(d => d.id === symbolId)) || null;

            if (!newItem) return oldItem;

            // Freshness check for symbol
            if (!isMorningOpening && oldItem && newItem.time && newItem.time < oldItem.time) {
                return oldItem;
            }

            // Stability check bypass during opening
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
                            if (trk.count < STABILITY_COUNT_REQUIRED && (currentTimestamp - trk.startTime) < 45000) return oldItem;
                            delete jumpTracker[symbolId];
                        }
                    } else { delete jumpTracker[symbolId]; }
                }
            }

            let mergedItem = oldItem ? { ...oldItem } : { ...newItem };
            let priceChanged = oldItem && (oldItem.ltp !== newItem.ltp || oldItem.bid !== newItem.bid || oldItem.ask !== newItem.ask);

            ['ltp', 'bid', 'ask', 'high', 'low', 'close'].forEach(field => {
                if (!isInvalid(newItem[field])) mergedItem[field] = newItem[field];
            });

            let processedTime = getProcessedTime(newItem.time || oldItem?.time);
            if ((timeNowStr > "23:30:00" || timeNowStr < "08:59:56") && !priceChanged && oldItem) {
                mergedItem.time = oldItem.time;
            } else {
                mergedItem.time = processedTime;
            }

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
        let displayGlobalTime = getProcessedTime(globalLatestTimeStr);
        finalResult.push({ id: "TIME", time: displayGlobalTime, ltp: "0.00", pcnt_chg: "0.00" });

        cachedData = { status: "success", data: finalResult };
        broadcastData(cachedData); 
        console.log(`Source: ${isBeforeMarket ? 'API' : 'GAS'} | Display Time: ${displayGlobalTime}`);

    } catch (err) {
        console.log(`Fetch failed for ${targetUrl}`);
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

// Start background task
setInterval(updateCacheInBackground, KATAULA_TIME);

// Initial bootstrap call to fill cache immediately on startup
updateCacheInBackground();

app.get('/flight-data', (req, res) => res.json(cachedData));

server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server Live on port ${PORT}`);
});

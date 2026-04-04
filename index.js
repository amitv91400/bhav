const express = require('express');
const axios = require('axios');
const compression = require('compression');
const http = require('http'); 
const WebSocket = require('ws'); 
const zlib = require('zlib'); 
const app = express();

app.use(compression());

app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*"); 
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

const KATAULA_TIME = 2500; 
let cachedData = { status: "success", data: [] }; 
let jumpTracker = {}; 
let globalLatestTimeStr = "00:00:00"; 
let currentIndex = 0; 

// --- Delta Tracking Variable ---
let lastBroadcastedDataMap = new Map(); 

const JUMP_THRESHOLD = 0.30; 
const STABILITY_COUNT_REQUIRED = 8; 

const allowedSymbols = ["MGLD-BA", "GLMM", "GOLDGUINEA", "GOLDPETAL", "MSIL-BA", "SLMM-BA", "SILMIC-BA", "MCRO-BA", "CRUDEOILM", "MNAG-BA", "NATGASMINI", "MALU", "ALUMINI", "MCOP-BA", "MLEA", "LEADMINI", "MZIN", "ZINCMINI", "ELECDMBL", "MMENO", "MUSDINR", "MGLD#", "GLMM#", "GOLDGUINEA#", "GOLDPETAL#", "MSIL#", "SLMM#", "SILMIC#", "MCRO#", "CRUDEOILM#", "MNAG#", "MALU#", "MCOP#", "MLEA#", "MZIN#"];

let gasUrls = [
    { id: 0, url: 'https://script.googleusercontent.com/macros/echo?user_content_key=AY5xjrRUAllQ4kn_3jhjXP73-_al7NC40f6I_9lZOL16pYNBdmNbvcu-iQctm1HeY4cYwepvqoY_uWQRh1xj-1o6fi7poKFX8ioydt_RVGda5uKkUYP3alEAifn0b9ywoPIxLpRdAZU0oe4HqwteTnlxvXh5J7JWU5g_vrAC9quoCY9mtSJzEqhoP-imOeG3loTRTRglUlsk6x4sMx9KV6Re7dGm6ueZulf5y5wRXIkyZTgo8Jwle29NV5nTgtK-RvdCrg56Y7asAkF1u2gqOY6nMBJbJY64Fg&lib=M2R5qj_HkbxQ248ypzLhX1ibLbEidnIEv' },
    { id: 1, url: 'https://script.googleusercontent.com/macros/echo?user_content_key=AY5xjrR2JxmFyCYWAOIiKyJjXLY5PQAsoZRdpxhnoD7KMMk0mIWJ3bBGf-qYbwK2-8YQTctl97vDnKUoizEzW67YdYVnFJaV8flnYMaF6kUcDm_fDjG4v_uOj1Tno3V4qSCzPADlkeDA00sK15TfT9fObv2TFv0TYwEnKu8_c-I6JVQCpB2K_VUr4_uDWu6EnKu8_c-I6JVQCpB2K_VUr4_uDWu6ec1pub_NFUuzWReFBb81jBQujBekZ9HdfrRxHffBz9RJhL4NZjUpLfCjnn0vHFVW_UX7Ra_A1LN1lzVL0iwCzUzpsMk2KtjUxsw&lib=MEdFWXmkvh97tiV9iOiWZA6NKe58B53cM' },
    { id: 2, url: 'https://script.googleusercontent.com/macros/echo?user_content_key=AY5xjrR920S5GbHUzxyP6nqOD7bvF4txmyPwpCeScUKtFHbeQlf0G9BUBh2sHkrjJgAC569Jd-FB4mOYJFvkuA0dC5ATXgTKxqVY_7ZQc8tUBnV8iZW5Ud8tefHkgl1H8pDoHrCznyUgjV4S1o_5hhCBJ-uBUBJXWGDYppKyU-xkmTPK71rYbPE2qjKIXdrb7byMDBLhrIQhD6zcWfgw4NfyqajEk7QSb_269Qf0-UaVmi92d9qJIbG84PatYgfIayZ19GTCp4pj2v9kJ5pNsigghmk4PaEJ5i1eDBweEDSv&lib=MCfXe93vS6bvNJsCX9fU1g-9rjzA1OwSu' },
    { id: 3, url: 'https://script.googleusercontent.com/macros/echo?user_content_key=AY5xjrSbfLWmiJlZfZSMiVHrMi7ZCgMYLq1rwWgfX3gPoCQFktvEAeA-9NDdHbVkdV_zloksAsDx1oTBwjyWJzu0CWtEUCKp0VR5Rp5ctruOy5xY8yhy5j-n4fkZMt9OHed14IFPRUzKDfPgZ9BDhwnSRxKiZo66833QmJgCh-uXkhBfxGl9cZhGq2HfDqQxtmEgSJzEqhoP-imOeG3loTRTRglUlsk6x4sMx9KV6Re7dGm6ueZulf5y5wRXIkyZTgo8Jwle29NV5nTgtK-RvdCrg56Y7asAkF1u2gqOY6nMBJbJY64Fg&lib=MPF7r8_r4M65t9LeV_2zbyL3Igg1YIVKh' },
    { id: 4, url: 'https://script.googleusercontent.com/macros/echo?user_content_key=AWDtjMWDqBz7ti8amYLaqt3xaqp7u89FbroeHZ9iAul6I_KHOXTt_0LUfXn2Axnvj2iufs2v-wXkX2HOP4chUlRZjDhMWYtOQFpWLMllZINduoh8Bo6GRn8BlWeS1tet02lTiKCFbuWg1YUnLRJPxK6rgZ0QBhoeLt5A-x7rL__TQHMs3nmTntdJ1rRWvAftUExGJGKKEOL6eLp3Nuv8JZ7vBShEQw_JxqAVNXry1DeHsszBawFPxVllJuEoYXW4AK-b_jq3WwsLQ97agbS7XlrnldntKXXv3g&lib=MJZiqDr5C4M89dJomwQH7Xk1ne2cgL6PN' },
    { id: 5, url: 'https://script.googleusercontent.com/macros/echo?user_content_key=AWDtjMU3yrKZjPdxFxlGMz2N7q5wvK4RvTJdsufArOZWq1svrarcK3YuY6WpQd9t-HN4t181FCrOFfgQKoFSwsDIflYS1DahaqYJtVdv392k17XBUiT2b8EfGFeYpSNdGB6LTYJm2j6IDQvR9_FRddH0VqBCwBa_QznheL3NQ8NHhWzoj9c67zozJ0xwBo_Ob8EsLtWwNPZ6QGXosUl2bdFARmq5sbzm7WgDGCKr-xjuEXQKx969CWrYgfbYBpqE3nNYDjTG8p2gb4aXDXzRtm2e3uxPR9mLfoFky2nGGLrV&lib=MA_FDe4_RAifqTi8OdSnJvc6m7vGK6F0l' },
    { id: 6, url: 'https://script.googleusercontent.com/macros/echo?user_content_key=AWDtjMUbayxxu43zQ5rujTdD21214ODi4Jwv6227GfrSY7i0BzLhgiyV9pZn76kKaQyb47OW85PpNB9Z7PIneb2C3iRp3Ps3vZMCJQJg17aDYOUyAJejYMQ-ZJkAjvTsh-89KJ_kT0XnxH_h5LZTwRZPOu9ck0pv_Kc5X8JTR0Aqv5JrYsk1ntbfdqWh7Tr15GLCLBUDXtUYQFS8fLhObI6Jz6IPbyJZRRiQdVT5EaKgYcO1IL7W5csZddWeELNy3Xb_eZy5lapBkDaVujUsvd5bxlReTFpR4Q&lib=MT8Cc-Zmmv1rm1E-0-oIrAamwDtmjqP8O' },
    { id: 7, url: 'https://script.googleusercontent.com/macros/echo?user_content_key=AWDtjMV5fd8X3kd6vWyK2jVJuWBnkPtALwaRWVqKXjGLskaMFHy6SKRmRkL2bmQfequw90y7jk64I5k9CDkzMmHoXXJ5UtJ1HPTfBqqUlv0JQ0k_Yu6HLrAdmY7SaooEXM5XWQ4RzuttTGZo2UfQa15enFi5ZkbHxEHiyVxaEylUaCyHZLf7pz1Rx8UM4DnELJt68sT1n5oAuzt_kxBgY-vvOKMxbEz1mjX6o7zv8gDZWAqqMoWJRPfdC16sV-Sq5tMuzL3NyS2B5GuHBGywUB1vZmfiT4ZMpQ&lib=MKe2s3kAIog9mX9FQEPEJxBSBcQ8BraCT' }
];

function isInvalid(val) {
    return val === null || val === undefined || val === "" || val === "0" || val === "0.00" || val === "0000" || val === "----" || isNaN(parseFloat(val));
}

function getISTTime() {
    return new Date(new Date().toLocaleString("en-US", {timeZone: "Asia/Kolkata"}));
}

async function fetchFromGas(gasObj) {
    const res = await axios.get(gasObj.url, { timeout: 9000 });
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

        if (incomingMaxTimeStr < globalLatestTimeStr && globalLatestTimeStr !== "00:00:00") {
            return; 
        }

        const finalDataList = allowedSymbols.map(symbolId => {
            const newItem = newData.data.find(d => d.id === symbolId);
            const oldItem = (cachedData.data && cachedData.data.find(d => d.id === symbolId)) || null;

            if (!newItem) return oldItem;

            if (oldItem && !isMorningOpening) {
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
                    } else {
                        delete jumpTracker[symbolId];
                    }
                }
            } else if (isMorningOpening) {
                delete jumpTracker[symbolId];
            }

            let mergedItem = oldItem ? { ...oldItem } : { ...newItem };
            ['ltp', 'bid', 'ask', 'high', 'low', 'close', 'time'].forEach(field => {
                if (!isInvalid(newItem[field])) mergedItem[field] = newItem[field];
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

        globalLatestTimeStr = incomingMaxTimeStr;
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

// रेंडर के लिए पोर्ट सेटिंग्स (बदलाव यहाँ है)
const PORT = process.env.PORT || 8080;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server Live on ${PORT} - Level 1 Delta Tracking Active`);
});
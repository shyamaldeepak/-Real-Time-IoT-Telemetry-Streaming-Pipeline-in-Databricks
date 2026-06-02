// Pipeline Simulation State
let isRunning = false;
let intervalId = null;
let speed = 3000; // ms

let totalRawCount = 0;
let totalCleanCount = 0;
let totalFilteredCount = 0;
let totalAlertsCount = 0;

let fileCounter = 0;
let filesList = [];
let bronzeData = [];
let silverData = [];
let goldData = [];

// Device telemetry history for charts (stores last 15 points)
const deviceHistory = {
  "sensor-north-01": [],
  "sensor-east-02": [],
  "sensor-south-03": [],
  "sensor-west-04": []
};

// Colors for devices on chart
const deviceColors = {
  "sensor-north-01": "#3b82f6", // Blue
  "sensor-east-02": "#10b981",  // Green
  "sensor-south-03": "#ec4899", // Pink
  "sensor-west-04": "#f59e0b"  // Orange
};

const devices = Object.keys(deviceHistory);

// DOM Elements
const btnPlayPause = document.getElementById("btn-play-pause");
const btnInjectAnomaly = document.getElementById("btn-inject-anomaly");
const btnInjectCorrupt = document.getElementById("btn-inject-corrupt");
const btnReset = document.getElementById("btn-reset");
const selectSpeed = document.getElementById("select-speed");

const countLanding = document.getElementById("count-landing");
const countBronze = document.getElementById("count-bronze");
const countSilver = document.getElementById("count-silver");
const countGold = document.getElementById("count-gold");

const listLandingFiles = document.getElementById("list-landing-files");
const tblBronze = document.getElementById("tbl-bronze").querySelector("tbody");
const tblSilver = document.getElementById("tbl-silver").querySelector("tbody");
const tblGold = document.getElementById("tbl-gold").querySelector("tbody");

const metricTotalRaw = document.getElementById("metric-total-raw");
const metricTotalClean = document.getElementById("metric-total-clean");
const metricTotalFiltered = document.getElementById("metric-total-filtered");
const metricTotalAlerts = document.getElementById("metric-total-alerts");

const consoleExplanation = document.getElementById("console-explanation");

// Canvas Chart Configuration
const canvas = document.getElementById("chart-realtime");
const ctx = canvas.getContext("2d");

// Event Flags
let nextRecordAnomaly = false;
let nextRecordCorrupt = false;

// Explanations queue to display to the user
const explanations = {
  start: "🚀 Ingestion started! Auto Loader (cloudFiles) is actively polling the landing directory. It reads new files incrementally, meaning we only pay cloud providers for what we process.",
  file: (name) => `📂 Ingested new file: <strong>${name}</strong>. Inside Databricks, Auto Loader appends metadata like the file path and arrival time. These raw records are directly dumped into the <strong>Bronze Table</strong>.`,
  clean: "🥈 Clean logic executed! The Silver stream has cast string temperatures to decimals and filtered out corrupt data. The <strong>Silver Table</strong> is now our clean Single Source of Truth.",
  corrupt: "💀 Data Quality Check failed: A corrupted record was detected (Null Humidity). The Silver table filter immediately dropped it. This prevents downstream machine learning models or reports from crashing!",
  window: "🥇 Aggregation Triggered: Structured Streaming calculated 5-minute sliding averages. The watermark handles late data and prevents the server memory from filling up indefinitely.",
  anomaly: "🔥 SAFETY ALERT! A temperature of over 80.0°C was detected in the Gold Layer. An alert flag is set to <strong>'CRITICAL'</strong>. In production, this can trigger an SMS or Email notification to the maintenance engineer."
};

// Initialize Chart canvas sizes
function resizeCanvas() {
  const rect = canvas.parentNode.getBoundingClientRect();
  canvas.width = rect.width;
  canvas.height = 280;
  drawChart();
}
window.addEventListener('resize', resizeCanvas);
setTimeout(resizeCanvas, 100);

// Setup Event Listeners
btnPlayPause.addEventListener("click", toggleSimulation);
btnInjectAnomaly.addEventListener("click", () => {
  nextRecordAnomaly = true;
  updateConsole("🔥 An anomaly has been queued. The next telemetry record will simulate a hardware failure (>80°C). Watch the Gold Layer table!");
});
btnInjectCorrupt.addEventListener("click", () => {
  nextRecordCorrupt = true;
  updateConsole("💀 Corrupted data has been queued. The next file will contain a record with a null humidity value. Watch how the Silver layer reacts!");
});
btnReset.addEventListener("click", resetPipeline);
selectSpeed.addEventListener("change", (e) => {
  speed = parseInt(e.target.value);
  if (isRunning) {
    stopInterval();
    startInterval();
  }
});

// Update explaining console
function updateConsole(text) {
  consoleExplanation.innerHTML = text;
  consoleExplanation.style.border = "1px solid #3b82f6";
  consoleExplanation.style.boxShadow = "0 0 10px rgba(59, 130, 246, 0.2)";
  setTimeout(() => {
    consoleExplanation.style.border = "1px solid rgba(59, 130, 246, 0.2)";
    consoleExplanation.style.boxShadow = "none";
  }, 1000);
}

// Simulation Interval Controllers
function startInterval() {
  intervalId = setInterval(processNextBatch, speed);
}

function stopInterval() {
  clearInterval(intervalId);
}

function toggleSimulation() {
  isRunning = !isRunning;
  if (isRunning) {
    btnPlayPause.classList.remove("btn-primary");
    btnPlayPause.classList.add("btn-secondary");
    btnPlayPause.querySelector(".btn-icon").textContent = "⏸";
    btnPlayPause.querySelector(".btn-text").textContent = "Pause Ingestion";
    updateConsole(explanations.start);
    startInterval();
  } else {
    btnPlayPause.classList.add("btn-primary");
    btnPlayPause.classList.remove("btn-secondary");
    btnPlayPause.querySelector(".btn-icon").textContent = "▶";
    btnPlayPause.querySelector(".btn-text").textContent = "Start Ingestion";
    updateConsole("⏸ Ingestion paused. Live streams in Databricks will hold their check-pointed positions and resume exactly where they left off when started again.");
    stopInterval();
  }
}

// Generate Mock Record
function generateRecord() {
  const device = devices[Math.floor(Math.random() * devices.length)];
  let temp, hum, status;

  if (nextRecordAnomaly) {
    temp = parseFloat((Math.random() * 15 + 80).toFixed(2)); // 80 - 95 C
    status = "WARNING";
    nextRecordAnomaly = false;
  } else {
    temp = parseFloat((Math.random() * 14 + 18).toFixed(2)); // 18 - 32 C
    status = "OK";
  }

  if (nextRecordCorrupt) {
    hum = null;
    nextRecordCorrupt = false;
  } else {
    hum = parseFloat((Math.random() * 30 + 35).toFixed(2)); // 35 - 65%
  }

  const now = new Date();
  return {
    device_id: device,
    timestamp: now.toISOString(),
    temperature: temp,
    humidity: hum,
    status: status
  };
}

// Core pipeline logic triggered every interval
function processNextBatch() {
  fileCounter++;
  const filename = `telemetry_${Math.floor(Date.now()/1000)}_${fileCounter}.json`;
  
  // 1. Landing Zone File Write
  filesList.push(filename);
  if (filesList.length > 5) filesList.shift(); // keep last 5 files visible
  
  // Update Landing zone UI
  listLandingFiles.innerHTML = "";
  filesList.forEach(f => {
    const div = document.createElement("div");
    div.className = "file-item";
    div.textContent = f;
    listLandingFiles.appendChild(div);
  });
  countLanding.textContent = `${fileCounter} Files Ingested`;
  updateConsole(explanations.file(filename));

  // Generate 3 readings for this file
  const batch = [generateRecord(), generateRecord(), generateRecord()];
  
  // Flags to check what happened in this batch
  let hadCorrupt = false;
  let hadAnomaly = false;

  // Process each record in the batch
  batch.forEach(rec => {
    totalRawCount++;
    
    // --- 2. Bronze Layer (Ingest everything + append audit metadata) ---
    const bronzeRec = {
      ...rec,
      _input_file_name: `dbfs:/landing_zone/${filename}`,
      _ingested_time: new Date().toLocaleTimeString()
    };
    bronzeData.unshift(bronzeRec);
    if (bronzeData.length > 50) bronzeData.pop();

    // --- 3. Silver Layer (Data Cleansing & Types) ---
    // If humidity is null, we filter it out (Corrupt record)
    if (rec.humidity === null) {
      totalFilteredCount++;
      hadCorrupt = true;
    } else {
      totalCleanCount++;
      const silverRec = {
        device_id: rec.device_id,
        timestamp: rec.timestamp,
        temperature: parseFloat(rec.temperature.toFixed(1)), // rounding temp
        humidity: parseFloat(rec.humidity.toFixed(1)),
        status: rec.status
      };
      silverData.unshift(silverRec);
      if (silverData.length > 50) silverData.pop();

      // Add to device history for real-time charting
      deviceHistory[rec.device_id].push({
        time: new Date(rec.timestamp),
        temp: silverRec.temperature
      });
      if (deviceHistory[rec.device_id].length > 15) {
        deviceHistory[rec.device_id].shift();
      }

      // --- 4. Gold Layer (Windowed aggregations & Alerts) ---
      // We calculate a rolling average for the device over the last few readings
      const deviceHistoryList = deviceHistory[rec.device_id];
      const temps = deviceHistoryList.map(h => h.temp);
      const hums = deviceHistoryList.map(h => h.temp); // Mock hum
      
      const avgTemp = parseFloat((temps.reduce((a,b)=>a+b, 0) / temps.length).toFixed(1));
      const maxTemp = Math.max(...temps);
      const avgHum = parseFloat((hums.reduce((a,b)=>a+b, 0) / hums.length).toFixed(1));
      
      const wStart = new Date(new Date(rec.timestamp).getTime() - 5*60*1000).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit', second:'2-digit'});
      const wEnd = new Date(rec.timestamp).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit', second:'2-digit'});
      
      let alertStatus = "NORMAL";
      if (maxTemp > 80.0) {
        alertStatus = "CRITICAL";
        totalAlertsCount++;
        hadAnomaly = true;
      }

      // Check if device already has a window row in Gold
      const existingGoldIdx = goldData.findIndex(g => g.device_id === rec.device_id);
      const goldRec = {
        device_id: rec.device_id,
        window_start: wStart,
        window_end: wEnd,
        avg_temperature: avgTemp,
        max_temperature: maxTemp,
        alert_status: alertStatus
      };

      if (existingGoldIdx !== -1) {
        // Update window
        goldData[existingGoldIdx] = goldRec;
      } else {
        goldData.unshift(goldRec);
      }
    }
  });

  // Update UI Tables
  updateBronzeTable();
  updateSilverTable();
  updateGoldTable();

  // Update Metrics
  metricTotalRaw.textContent = totalRawCount;
  metricTotalClean.textContent = totalCleanCount;
  metricTotalFiltered.textContent = totalFilteredCount;
  metricTotalAlerts.textContent = totalAlertsCount;

  // Update Active device status panel
  updateDeviceCards();

  // Draw chart
  drawChart();

  // Update Console messaging depending on what occurred in the batch
  setTimeout(() => {
    if (hadAnomaly) {
      updateConsole(explanations.anomaly);
    } else if (hadCorrupt) {
      updateConsole(explanations.corrupt);
    } else {
      const randomMsg = Math.random();
      if (randomMsg < 0.4) {
        updateConsole(explanations.clean);
      } else if (randomMsg < 0.8) {
        updateConsole(explanations.window);
      }
    }
  }, 1200);
}

// Table UI Builders
function updateBronzeTable() {
  if (bronzeData.length === 0) {
    tblBronze.innerHTML = `<tr class="placeholder-row"><td colspan="6">No raw data ingested yet.</td></tr>`;
    countBronze.textContent = "0 rows";
    return;
  }
  
  countBronze.textContent = `${totalRawCount} rows`;
  tblBronze.innerHTML = "";
  bronzeData.slice(0, 5).forEach((row, idx) => {
    const tr = document.createElement("tr");
    if (idx === 0) tr.className = "new-row";
    
    // Highlight if corrupt (humidity null) or anomaly (temp > 80)
    let tempStyle = "";
    if (row.temperature > 80) tempStyle = 'class="text-danger font-bold"';
    let humVal = row.humidity !== null ? row.humidity : '<span class="corrupt-highlight">null</span>';
    let statusBadge = row.status === "WARNING" ? `<span class="badge-alert">WARN</span>` : `<span>${row.status}</span>`;

    tr.innerHTML = `
      <td><strong>${row.device_id}</strong></td>
      <td>${new Date(row.timestamp).toLocaleTimeString()}</td>
      <td ${tempStyle}>${row.temperature}</td>
      <td>${humVal}</td>
      <td>${statusBadge}</td>
      <td style="color:#64748b; font-size:0.65rem;">${row._input_file_name.substring(19)}</td>
    `;
    tblBronze.appendChild(tr);
  });
}

function updateSilverTable() {
  if (silverData.length === 0) {
    tblSilver.innerHTML = `<tr class="placeholder-row"><td colspan="5">No clean data processed yet.</td></tr>`;
    countSilver.textContent = "0 rows";
    return;
  }
  
  countSilver.textContent = `${totalCleanCount} rows`;
  tblSilver.innerHTML = "";
  silverData.slice(0, 5).forEach((row, idx) => {
    const tr = document.createElement("tr");
    if (idx === 0) tr.className = "new-row";
    
    let tempStyle = row.temperature > 80 ? 'class="text-danger font-bold"' : "";
    let statusBadge = row.status === "WARNING" ? `<span class="badge-alert">WARN</span>` : `<span class="text-success">OK</span>`;
    
    tr.innerHTML = `
      <td><strong>${row.device_id}</strong></td>
      <td>${new Date(row.timestamp).toLocaleTimeString()}.${new Date(row.timestamp).getMilliseconds()}</td>
      <td ${tempStyle}>${row.temperature}</td>
      <td>${row.humidity}</td>
      <td>${statusBadge}</td>
    `;
    tblSilver.appendChild(tr);
  });
}

function updateGoldTable() {
  if (goldData.length === 0) {
    tblGold.innerHTML = `<tr class="placeholder-row"><td colspan="6">No windowed aggregates calculated yet.</td></tr>`;
    countGold.textContent = "0 rows";
    return;
  }
  
  countGold.textContent = `${goldData.length} aggregates`;
  tblGold.innerHTML = "";
  goldData.slice(0, 5).forEach((row, idx) => {
    const tr = document.createElement("tr");
    
    let alertBadge = "";
    if (row.alert_status === "CRITICAL") {
      tr.className = "highlight-alert";
      alertBadge = `<span class="badge-alert pulse-text">🚨 CRITICAL</span>`;
    } else {
      if (idx === 0) tr.className = "new-row";
      alertBadge = `<span class="badge-normal">NORMAL</span>`;
    }
    
    tr.innerHTML = `
      <td><strong>${row.device_id}</strong></td>
      <td>${row.window_start}</td>
      <td>${row.window_end}</td>
      <td>${row.avg_temperature}</td>
      <td>${row.max_temperature}</td>
      <td>${alertBadge}</td>
    `;
    tblGold.appendChild(tr);
  });
}

// Active devices UI status
function updateDeviceCards() {
  devices.forEach(dev => {
    const card = document.getElementById(`card-${dev}`);
    const history = deviceHistory[dev];
    
    if (history && history.length > 0) {
      const latest = history[history.length - 1];
      const tempSpan = card.querySelector(".val-temp");
      const humSpan = card.querySelector(".val-hum");
      const indicator = card.querySelector(".status-indicator");
      
      tempSpan.textContent = latest.temp;
      // Get silver record for humidity
      const silRec = silverData.find(s => s.device_id === dev);
      humSpan.textContent = silRec ? silRec.humidity : "--";

      if (latest.temp > 80.0) {
        indicator.className = "status-indicator stat-danger";
        indicator.textContent = "● OVERHEAT ALERT";
        card.style.borderColor = "#ef4444";
        card.style.background = "rgba(239, 68, 68, 0.05)";
      } else if (latest.temp > 30.0) {
        indicator.className = "status-indicator stat-warn";
        indicator.textContent = "● WARM";
        card.style.borderColor = "#f59e0b";
        card.style.background = "rgba(245, 158, 11, 0.05)";
      } else {
        indicator.className = "status-indicator stat-ok";
        indicator.textContent = "● OK";
        card.style.borderColor = "rgba(255, 255, 255, 0.05)";
        card.style.background = "rgba(15, 23, 42, 0.4)";
      }
    }
  });
}

// Custom 2D HTML5 Canvas Line Chart Renderer
function drawChart() {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  
  const paddingLeft = 40;
  const paddingRight = 130;
  const paddingTop = 20;
  const paddingBottom = 30;
  
  const plotWidth = canvas.width - paddingLeft - paddingRight;
  const plotHeight = canvas.height - paddingTop - paddingBottom;

  // Draw grid lines
  ctx.strokeStyle = "rgba(255, 255, 255, 0.05)";
  ctx.lineWidth = 1;
  
  // Horizontal grids (Temperatures from 0C to 100C)
  const tempMin = 0;
  const tempMax = 100;
  const gridLines = 5;
  for (let i = 0; i <= gridLines; i++) {
    const tempVal = tempMin + (i * (tempMax - tempMin) / gridLines);
    const y = paddingTop + plotHeight - (i * plotHeight / gridLines);
    
    ctx.beginPath();
    ctx.moveTo(paddingLeft, y);
    ctx.lineTo(paddingLeft + plotWidth, y);
    ctx.stroke();
    
    // Label
    ctx.fillStyle = "#64748b";
    ctx.font = "10px Outfit";
    ctx.textAlign = "right";
    ctx.fillText(`${tempVal}°C`, paddingLeft - 8, y + 3);
  }

  // Draw border outline
  ctx.strokeStyle = "rgba(255, 255, 255, 0.1)";
  ctx.strokeRect(paddingLeft, paddingTop, plotWidth, plotHeight);

  // Draw lines for each device
  devices.forEach((dev, devIdx) => {
    const history = deviceHistory[dev];
    if (history.length < 2) return;

    ctx.beginPath();
    ctx.strokeStyle = deviceColors[dev];
    ctx.lineWidth = 2.5;
    ctx.lineJoin = "round";

    history.forEach((pt, ptIdx) => {
      const x = paddingLeft + (ptIdx * plotWidth / 14); // spread 15 elements across width
      const y = paddingTop + plotHeight - ((pt.temp - tempMin) * plotHeight / (tempMax - tempMin));
      
      if (ptIdx === 0) {
        ctx.moveTo(x, y);
      } else {
        ctx.lineTo(x, y);
      }
    });
    ctx.stroke();

    // Draw last point circle
    const lastPt = history[history.length - 1];
    const lastX = paddingLeft + ((history.length - 1) * plotWidth / 14);
    const lastY = paddingTop + plotHeight - ((lastPt.temp - tempMin) * plotHeight / (tempMax - tempMin));
    
    ctx.beginPath();
    ctx.fillStyle = deviceColors[dev];
    ctx.arc(lastX, lastY, 4, 0, Math.PI * 2);
    ctx.fill();
  });

  // Draw Right-aligned Legends
  ctx.textAlign = "left";
  devices.forEach((dev, devIdx) => {
    const x = paddingLeft + plotWidth + 15;
    const y = paddingTop + 15 + (devIdx * 22);

    // Color dot
    ctx.beginPath();
    ctx.fillStyle = deviceColors[dev];
    ctx.arc(x, y - 4, 5, 0, Math.PI * 2);
    ctx.fill();

    // Name and latest temp value
    const history = deviceHistory[dev];
    const latestVal = history.length > 0 ? `${history[history.length - 1].temp}°C` : "--";
    ctx.fillStyle = "#cbd5e1";
    ctx.font = "11px Outfit";
    ctx.fillText(`${dev}: ${latestVal}`, x + 12, y);
  });

  // Label bottom axis
  ctx.fillStyle = "#64748b";
  ctx.font = "10px Outfit";
  ctx.textAlign = "center";
  ctx.fillText("Real-Time Telemetry Data Window Stream (15 active points)", paddingLeft + (plotWidth / 2), canvas.height - 8);
}

// Reset Pipeline
function resetPipeline() {
  const confirmReset = confirm("Are you sure you want to clear the landing zone and drop all Delta tables? This deletes current states.");
  if (!confirmReset) return;

  if (isRunning) toggleSimulation();

  fileCounter = 0;
  totalRawCount = 0;
  totalCleanCount = 0;
  totalFilteredCount = 0;
  totalAlertsCount = 0;

  filesList = [];
  bronzeData = [];
  silverData = [];
  goldData = [];

  devices.forEach(dev => {
    deviceHistory[dev] = [];
    
    const card = document.getElementById(`card-${dev}`);
    card.querySelector(".val-temp").textContent = "--";
    card.querySelector(".val-hum").textContent = "--";
    const indicator = card.querySelector(".status-indicator");
    indicator.className = "status-indicator stat-ok";
    indicator.textContent = "● OK";
    card.style.borderColor = "rgba(255, 255, 255, 0.05)";
    card.style.background = "rgba(15, 23, 42, 0.4)";
  });

  // Reset metrics UI
  metricTotalRaw.textContent = "0";
  metricTotalClean.textContent = "0";
  metricTotalFiltered.textContent = "0";
  metricTotalAlerts.textContent = "0";

  updateBronzeTable();
  updateSilverTable();
  updateGoldTable();
  drawChart();

  updateConsole("🔄 Pipeline directories reset. All Delta logs and tables have been successfully cleared. Ready for fresh stream ingestion.");
}

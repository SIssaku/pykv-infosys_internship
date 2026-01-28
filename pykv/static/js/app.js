// Status message element
const statusBox = document.getElementById("statusBox");

// Show a success or error status message
function showStatus(msg, ok=true) {
  if (!statusBox) return;
  statusBox.innerText = msg;
  statusBox.style.color = ok ? "green" : "red";
}

// ---------------- SET KEY ----------------
async function setKey() {
  const key = document.getElementById("key").value.trim();
  const value = document.getElementById("value").value.trim();
  const ttlRaw = document.getElementById("ttl").value.trim();
  const ttl = ttlRaw ? parseInt(ttlRaw) : null;

  const res = await fetch("/set", {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({key, value, ttl})
  });

  if(res.ok){
    showStatus("✅ Key saved successfully");
    loadKeys(); // refresh keys list
  } else {
    const data = await res.json();
    showStatus("❌ " + (data.detail || "Error"), false);
  }
}

// ---------------- GET KEY ----------------
async function getKey() {
  const key = document.getElementById("getKey").value.trim();
  const out = document.getElementById("getResult");

  const res = await fetch(`/get/${key}`);
  const data = await res.json();

  if(res.ok){
    out.innerText = JSON.stringify(data, null, 2);
    showStatus("✅ Key fetched successfully");
  } else {
    out.innerText = "Key not found";
    showStatus("❌ Key not found", false);
  }
}

// ---------------- DELETE KEY ----------------
async function deleteKey() {
  const key = document.getElementById("delKey").value.trim();

  const res = await fetch(`/delete/${key}`, {method: "DELETE"});
  const data = await res.json();

  if(res.ok){
    showStatus("✅ Key deleted: " + key);
    loadKeys(); // refresh
  } else {
    showStatus("❌ " + (data.detail || "Error"), false);
  }
}

// ---------------- LOAD KEYS ----------------
async function loadKeys() {
  const list = document.getElementById("keysList");
  if(!list) return;

  list.innerHTML = "";

  const res = await fetch("/keys");
  const data = await res.json();

  data.keys.forEach(k => {
    const li = document.createElement("li");
    li.innerText = k;

    // click a key -> auto fill GET field and fetch value
    li.onclick = () => {
      document.getElementById("getKey").value = k;
      getKey();
    };

    list.appendChild(li);
  });

  showStatus(`✅ Keys loaded (${data.count})`);
}

// ---------------- CLEAR ALL KEYS ----------------
async function clearAll() {
  const res = await fetch("/clear", {method:"DELETE"});
  const data = await res.json();

  if(res.ok){
    showStatus("✅ " + data.message);
    loadKeys();
    const out = document.getElementById("getResult");
    if(out) out.innerText = "";
  } else {
    showStatus("❌ Error clearing store", false);
  }
}

// ---------------- LOAD STATS ----------------
async function loadStats() {
  const out = document.getElementById("statsBox");

  const res = await fetch("/stats");
  const data = await res.json();

  out.innerText = JSON.stringify(data, null, 2);
}

// ---------------- COMPACT WAL ----------------
async function compactWAL(){
  const res = await fetch("/compact", {method:"POST"});
  const data = await res.json();
  alert(data.message || "WAL compacted");
  loadStats();
}

// Auto load keys on dashboard page
window.onload = () => {
  loadKeys();
};

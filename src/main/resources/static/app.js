const $ = (selector) => document.querySelector(selector);
const tokenKey = "taskforge.jwt";
let cursor = new Date(Date.now() - 60 * 60 * 1000).toISOString();
let timer = null;

function setAuthState() {
  const signedIn = !!sessionStorage.getItem(tokenKey);
  $("#auth").classList.toggle("hidden", signedIn);
  $("#app").classList.toggle("hidden", !signedIn);
  $("#logout").classList.toggle("hidden", !signedIn);
  if (signedIn) { refresh(); timer ||= setInterval(refresh, 1500); }
  else { clearInterval(timer); timer = null; $("#connection").textContent = "Signed out"; }
}

async function api(path, options = {}) {
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  const token = sessionStorage.getItem(tokenKey);
  if (token) headers.Authorization = `Bearer ${token}`;
  const response = await fetch(path, { ...options, headers });
  if (response.status === 401) { sessionStorage.removeItem(tokenKey); setAuthState(); throw new Error("Session expired. Sign in again."); }
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.message || `Request failed (${response.status})`);
  return body;
}

async function authenticate(register) {
  const form = new FormData($("#auth-form"));
  const path = register ? "/api/auth/register" : "/api/auth/login";
  try {
    const result = await api(path, { method: "POST", body: JSON.stringify({ email: form.get("email"), password: form.get("password") }) });
    sessionStorage.setItem(tokenKey, result.accessToken); $("#auth-error").textContent = ""; setAuthState();
  } catch (error) { $("#auth-error").textContent = error.message; }
}

function node(tag, text, className) { const element = document.createElement(tag); if (text != null) element.textContent = text; if (className) element.className = className; return element; }

function render(snapshot) {
  $("#connection").textContent = `LIVE · ${new Date(snapshot.serverTime).toLocaleTimeString()}`;
  $("#connection").className = "pill good";
  $("#worker-panel").classList.toggle("hidden", !snapshot.admin);
  const metrics = $("#metrics"); metrics.replaceChildren();
  ["SCHEDULED", "QUEUED", "RUNNING", "RETRYING", "SUCCESS", "FAILED"].forEach((status) => {
    const box = node("div", null, "metric"); box.append(node("span", status.replaceAll("_", " "))); box.append(node("strong", String(snapshot.counts[status] || 0))); metrics.append(box);
  });
  const tbody = $("#jobs"); tbody.replaceChildren();
  $("#job-count").textContent = `${snapshot.jobs.length} shown`;
  if (!snapshot.jobs.length) tbody.append(Object.assign(document.createElement("tr"), { innerHTML: '<td colspan="6" class="empty">No jobs yet. Create a demo job below.</td>' }));
  snapshot.jobs.forEach((job) => {
    const row = document.createElement("tr");
    const name = node("td"); const detailLink=node("button",`${job.name} · #${job.id}`,"button ghost small");detailLink.onclick=()=>showHistory(job);name.append(detailLink);
    row.append(name, node("td", job.type), Object.assign(node("td", job.status, `status ${job.status}`)));
    const worker = snapshot.workers.find((w) => w.currentJobId === job.id);
    const assignment = snapshot.events.filter((event) => event.jobId === job.id && event.workerId).at(-1);
    row.append(node("td", worker ? worker.hostname : (assignment?.workerId || "—")), node("td", `Run ${job.runNumber || "—"} · ${job.attemptCount}/${job.maxRetries + 1}`));
    const action = document.createElement("td");
    if (["CREATED", "SCHEDULED", "QUEUED", "RUNNING"].includes(job.status)) { const button = node("button", job.status === "RUNNING" ? "Request cancel" : "Cancel", "button small secondary"); button.onclick = () => cancelJob(job.id); action.append(button); }
    else if (job.cancellationRequested) action.textContent = "Cancel requested";
    row.append(action); tbody.append(row);
  });
  const workerList = $("#workers"); workerList.replaceChildren();
  if (!snapshot.workers.length) workerList.append(node("p", "No workers have registered yet.", "empty"));
  snapshot.workers.forEach((worker) => {
    const card = node("div", null, "worker"); card.append(node("strong", worker.hostname), node("span", worker.status, `status ${worker.status}`));
    card.append(node("small", worker.currentJobId ? `working on job #${worker.currentJobId}` : `last heartbeat ${new Date(worker.lastHeartbeat).toLocaleTimeString()}`)); workerList.append(card);
  });
  const eventList = $("#events");
  snapshot.events.forEach((event) => {
    if (eventList.querySelector(`[data-event="${event.id}"]`)) return;
    const item = node("li"); item.dataset.event = event.id;
    item.append(node("time", `${new Date(event.createdAt).toLocaleTimeString()} · JOB #${event.jobId} · ${event.status}`));
    item.append(document.createTextNode(`${event.detail || "Status changed"}${event.workerId ? ` · ${event.workerId}` : ""}`));
    eventList.prepend(item);
    while (eventList.childElementCount > 200) eventList.lastElementChild.remove();
  });
  if (snapshot.events.length) { const newest = snapshot.events.reduce((max, event) => Math.max(max, Date.parse(event.createdAt)), Date.parse(cursor)); cursor = new Date(newest).toISOString(); }
}

async function refresh() {
  if (!sessionStorage.getItem(tokenKey)) return;
  try { const data = await api(`/api/dashboard/snapshot?since=${encodeURIComponent(cursor)}`); render(data); }
  catch (error) { $("#connection").textContent = "API unavailable"; $("#connection").className = "pill warn"; console.error(error); }
}

async function cancelJob(id) {
  try { await api(`/api/jobs/${id}/cancel`, { method: "POST" }); await refresh(); }
  catch (error) { alert(error.message); }
}

async function showHistory(job) {
  $("#detail-title").textContent = `${job.name} · #${job.id}`; const list=$("#execution-list");list.replaceChildren();
  try { const history=await api(`/api/jobs/${job.id}/executions`); if(!history.length)list.append(node("p","No attempts yet.","empty")); history.forEach((attempt)=>{const card=node("article",null,"execution");card.append(node("strong",`Run ${attempt.runNumber} · Attempt ${attempt.attemptNumber} · ${attempt.status}`));card.append(node("p",`Worker: ${attempt.workerId||"—"} · Started: ${attempt.startedAt?new Date(attempt.startedAt).toLocaleString():"not started"} · Duration: ${attempt.durationMs??"—"} ms`));if(attempt.errorMessage)card.append(node("pre",attempt.errorMessage));list.append(card);}); $("#job-detail").showModal(); }
  catch(error){list.append(node("p",error.message,"error"));$("#job-detail").showModal();}
}

$("#auth-form").addEventListener("submit", (event) => { event.preventDefault(); authenticate(false); });
$("#register").addEventListener("click", () => authenticate(true));
$("#logout").addEventListener("click", () => { sessionStorage.removeItem(tokenKey); cursor = new Date(Date.now()-3600000).toISOString(); setAuthState(); });
$("#refresh").addEventListener("click", refresh);
$("#close-detail").addEventListener("click", () => $("#job-detail").close());
$("#job-form").elements.scheduleType.addEventListener("change", (event) => {
  $("#runat-field").classList.toggle("hidden", event.target.value !== "ONE_TIME");
  $("#cron-field").classList.toggle("hidden", event.target.value !== "CRON");
  $("#zone-field").classList.toggle("hidden", event.target.value !== "CRON");
});
$("#job-form").addEventListener("submit", async (event) => {
  event.preventDefault(); $("#job-error").textContent = "";
  try {
    const form = new FormData(event.currentTarget);
    const scheduleType = form.get("scheduleType");
    const runAt = form.get("runAt");
    const body = { name: form.get("name"), type: form.get("type"), priority: form.get("priority"), scheduleType, payload: JSON.parse(form.get("payload")), maxRetries: 3, timeoutSeconds: 60 };
    if (scheduleType === "ONE_TIME" && runAt) body.runAt = new Date(runAt).toISOString();
    if (scheduleType === "CRON") { body.cronExpression = form.get("cronExpression"); body.timeZone = form.get("timeZone"); }
    await api("/api/jobs", { method: "POST", body: JSON.stringify(body) }); await refresh();
  } catch (error) { $("#job-error").textContent = error.message; }
});
setAuthState();

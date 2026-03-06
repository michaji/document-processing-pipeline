let UI_STUB_DATA = [];

let activeItem = null;

// DOM Elements
const queueList = document.getElementById("queue-list");
const fieldsList = document.getElementById("fields-list");
const actionBar = document.getElementById("action-bar");
const viewerContent = document.querySelector(".viewer-content");

async function init() {
  await loadQueue();
  setupEventListeners();
  setupKeyboardShortcuts();
}

async function loadQueue() {
  try {
    const res = await fetch("/api/queue");
    if (res.ok) {
      UI_STUB_DATA = await res.json();
      renderQueue();
    }
  } catch (e) {
    console.error("Failed to load queue", e);
  }
}

function getPriorityColor(priority) {
  switch (priority) {
    case 'critical': return 'sla-critical';
    case 'warn': return 'sla-warn';
    case 'good': return 'sla-good';
    default: return 'sla-good';
  }
}

function getPriorityIcon(priority) {
  switch (priority) {
    case 'critical': return '<i data-lucide="alert-circle" style="width:14px"></i>';
    case 'warn': return '<i data-lucide="alert-triangle" style="width:14px"></i>';
    case 'good': return '<i data-lucide="check-circle" style="width:14px"></i>';
    default: return '';
  }
}

function renderQueue() {
  queueList.innerHTML = UI_STUB_DATA.map(item => `
    <li class="queue-item ${activeItem && activeItem.id === item.id ? 'active' : ''}" data-id="${item.id}">
      <div class="queue-item-header">
        <span class="doc-id">${item.id}</span>
        <span class="sla-badge ${getPriorityColor(item.priority)}">
          ${getPriorityIcon(item.priority)}
          ${item.sla_hours}h
        </span>
      </div>
      <div style="font-size: 0.8rem; color: var(--text-secondary); display: flex; align-items: center; gap: 0.25rem;">
        <i data-lucide="file" style="width:12px"></i> ${item.documentType}
      </div>
    </li>
  `).join('');

  lucide.createIcons();
}

function selectItem(id) {
  activeItem = UI_STUB_DATA.find(i => i.id === id);
  renderQueue();
  renderDocument();
  renderFields();
  actionBar.style.display = 'grid';
}

function renderDocument() {
  if (!activeItem) return;
  viewerContent.innerHTML = `
    <img src="${activeItem.previewImage}" alt="Document Preview" style="max-width: 100%; max-height: 100%; object-fit: contain; border-radius: 8px;">
  `;
}

function renderFields() {
  if (!activeItem) return;

  fieldsList.innerHTML = activeItem.fields.map(field => {
    const isLowConfidence = field.confidence < 0.8;
    const confidenceColor = isLowConfidence ? 'low' : 'good';
    const confidenceIcon = isLowConfidence ? 'alert-triangle' : 'check-circle';
    const percent = Math.round(field.confidence * 100);

    return `
      <div class="field-group ${isLowConfidence ? 'is-low-confidence' : ''}">
        <div class="field-label">
          <span>${field.label}</span>
          <span class="confidence-indicator ${confidenceColor}">
            <i data-lucide="${confidenceIcon}" style="width:14px; height: 14px;"></i>
            ${percent}%
          </span>
        </div>
        <div class="field-input-wrapper">
          <input type="${field.key.includes('amount') ? 'text' : 'text'}" 
                 class="input-control" 
                 value="${field.value}" 
                 data-key="${field.key}" />
        </div>
      </div>
    `;
  }).join('');

  lucide.createIcons();
}

function setupEventListeners() {
  queueList.addEventListener("click", (e) => {
    const itemEl = e.target.closest(".queue-item");
    if (itemEl) {
      selectItem(itemEl.dataset.id);
    }
  });

  const btnProcess = document.getElementById("btn-process");
  const btnReject = document.getElementById("btn-reject");

  if (btnProcess) {
    btnProcess.addEventListener("click", () => handleProcess("APPROVE/CORRECT"));
  }
  if (btnReject) {
    btnReject.addEventListener("click", () => handleProcess("REJECT"));
  }
}

async function handleProcess(action) {
  if (!activeItem) return;

  const corrections = {};
  let finalAction = action;
  if (action === "APPROVE/CORRECT" || action === "CORRECT") {
    // Capture user inputs in case of correction
    document.querySelectorAll(".input-control").forEach(inp => {
      corrections[inp.dataset.key] = inp.value;
    });
    finalAction = "CORRECT";
  }

  try {
    await fetch(`/api/queue/${activeItem.queue_id}/submit`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        user_id: "demo-reviewer",
        action: finalAction,
        corrections: Object.keys(corrections).length > 0 ? corrections : null,
        reason: ""
      })
    });
  } catch (err) {
    console.error(err);
  }

  // alert(`${action} performed for ${activeItem.id}`);
  // Remove from queue
  const index = UI_STUB_DATA.findIndex(i => i.id === activeItem.id);
  if (index > -1) UI_STUB_DATA.splice(index, 1);

  activeItem = null;
  renderQueue();
  viewerContent.innerHTML = `
    <div class="pdf-placeholder">
      <i data-lucide="check-circle" class="placeholder-icon" style="color: var(--status-good); opacity: 1;"></i>
      <p>Document processed successfully. Select next from queue.</p>
    </div>
  `;
  fieldsList.innerHTML = '';
  actionBar.style.display = 'none';
  lucide.createIcons();
}

function setupKeyboardShortcuts() {
  document.addEventListener("keydown", (e) => {
    if (!activeItem) return;

    // Ignore if typing inside input fields and it's not Enter/Esc combinations
    if (e.target.tagName === 'INPUT' && e.key !== 'Enter') return;

    if (e.key === "Enter" && (e.ctrlKey || e.metaKey || e.target.tagName !== 'INPUT')) {
      e.preventDefault();
      handleProcess("APPROVE/CORRECT");
    }

    if (e.key === "r" || e.key === "R") {
      // only trigger if not in input
      if (e.target.tagName !== 'INPUT') {
        e.preventDefault();
        handleProcess("REJECT");
      }
    }
  });
}

// Initialize App
document.addEventListener("DOMContentLoaded", init);

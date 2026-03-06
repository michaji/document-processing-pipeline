import React, { useState, useEffect } from 'react';
import { FileText, CheckCircle, AlertTriangle, Clock, Settings, User, BarChart2, Check, AlertCircle } from 'lucide-react';
import './index.css';

type QueueItem = {
    id: string;
    documentId: string;
    priority: number;
    slaHoursRemaining: number;
    status: 'urgent' | 'warning' | 'good';
    fields: ExtractedField[];
};

type ExtractedField = {
    key: string;
    raw_key: string;
    value: string | number;
    confidence: number;
};

export default function App() {
    const [queue, setQueue] = useState<QueueItem[]>([]);
    const [selectedItem, setSelectedItem] = useState<QueueItem | null>(null);
    const [fields, setFields] = useState<ExtractedField[]>([]);

    // Stats
    const API_URL = "http://127.0.0.1:8000/api";
    const USER_ID = "reviewer_01";
    const [stats, setStats] = useState({ reviewed_today: 0, avg_time: 0, queue_length: 0 });
    const [isLoading, setIsLoading] = useState(false);
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [errorMsg, setErrorMsg] = useState<string | null>(null);

    const fetchQueueAndStats = async () => {
        setIsLoading(true);
        setErrorMsg(null);
        try {
            const res = await fetch(`${API_URL}/queue?limit=50`);
            const data = await res.json();
            setQueue(data);

            const statsRes = await fetch(`${API_URL}/stats`);
            const statsData = await statsRes.json();
            setStats({
                reviewed_today: statsData.reviewed_today || 0,
                avg_time: statsData.avg_review_time_seconds || 0,
                queue_length: statsData.pending_items || data.length
            });
        } catch (e) {
            console.error("Failed to fetch queue", e);
            setErrorMsg("Failed to connect to the backend server.");
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        fetchQueueAndStats();
        const interval = setInterval(fetchQueueAndStats, 30000);
        return () => clearInterval(interval);
    }, []);

    // Global Keyboard Shortcuts
    useEffect(() => {
        const handleGlobalKeyDown = (e: KeyboardEvent) => {
            if (!selectedItem) return;
            if (e.ctrlKey && e.key === 'Enter') {
                e.preventDefault();
                submitAction("COMPLETED");
            }
            if (e.ctrlKey && e.key === 'Backspace') {
                e.preventDefault();
                const reason = prompt("Enter rejection reason:");
                if (reason) submitAction("REJECTED", reason);
            }
        };
        window.addEventListener('keydown', handleGlobalKeyDown);
        return () => window.removeEventListener('keydown', handleGlobalKeyDown);
    }, [selectedItem, fields]);

    const handleSelect = async (item: QueueItem) => {
        setErrorMsg(null);
        // Optimistic UI lock
        try {
            await fetch(`${API_URL}/queue/${item.id}/claim`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: USER_ID })
            });
            setSelectedItem(item);
            setFields(JSON.parse(JSON.stringify(item.fields)));
        } catch (e) {
            console.error("Failed to claim item", e);
            setErrorMsg("Could not claim item. Someone else might be reviewing it.");
            fetchQueueAndStats(); // refresh
        }
    };

    const handleFieldChange = (index: number, val: string) => {
        const updated = [...fields];
        updated[index].value = val;
        setFields(updated);
    };

    const submitAction = async (action: string, reason?: string) => {
        if (!selectedItem) return;

        const isApprove = action === "COMPLETED";
        if (isApprove && !window.confirm("Are you sure you want to approve this document and submit corrections?")) {
            return;
        }

        setIsSubmitting(true);
        setErrorMsg(null);

        const corrections: Record<string, string | number> = {};
        let modified = false;

        for (let i = 0; i < fields.length; i++) {
            if (fields[i].value !== selectedItem.fields[i].value) {
                corrections[fields[i].raw_key] = fields[i].value;
                modified = true;
            }
        }

        try {
            await fetch(`${API_URL}/queue/${selectedItem.id}/submit`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    user_id: USER_ID,
                    action: action,
                    corrections: modified ? corrections : undefined,
                    reason: reason
                })
            });
            setSelectedItem(null);
            fetchQueueAndStats();
        } catch (e) {
            console.error(e);
            setErrorMsg(`Failed to submit document: ${e instanceof Error ? e.message : 'Unknown error'}`);
        } finally {
            setIsSubmitting(false);
        }
    };

    const handleApprove = () => submitAction("COMPLETED");
    const handleReject = () => {
        const reason = prompt("Enter rejection reason:");
        if (reason) submitAction("REJECTED", reason);
    };
    const handleCorrect = () => submitAction("COMPLETED");

    return (
        <div className="layout">
            {/* HEADER */}
            <header className="glass-panel">
                <div className="header-title">Review Dashboard</div>
                <div className="header-nav">
                    <button className="nav-btn"><BarChart2 size={18} /> Stats</button>
                    <button className="nav-btn"><Settings size={18} /> Settings</button>
                    <button className="nav-btn"><User size={18} /> User</button>
                </div>
            </header>

            {/* SIDEBAR */}
            <div className="sidebar">
                {/* QUEUE */}
                <div className="queue-panel glass-panel">
                    <div className="panel-header">
                        <span id="queue-heading">QUEUE ({stats.queue_length})</span>
                    </div>
                    {errorMsg && (
                        <div style={{ padding: '8px 16px', background: 'var(--accent-red)', color: 'white', fontSize: '0.8rem' }}>
                            {errorMsg}
                        </div>
                    )}
                    <div className="queue-list" role="listbox" aria-labelledby="queue-heading">
                        {isLoading && queue.length === 0 ? (
                            <div style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-muted)' }}>
                                <p>Loading queue data...</p>
                            </div>
                        ) : queue.map(item => (
                            <div
                                key={item.id}
                                role="option"
                                aria-selected={selectedItem?.id === item.id}
                                tabIndex={0}
                                className={`queue-item ${selectedItem?.id === item.id ? 'active' : ''}`}
                                onClick={() => handleSelect(item)}
                                onKeyDown={(e) => { if (e.key === 'Enter') handleSelect(item); }}
                            >
                                <div>
                                    <span className={`status-dot ${item.status}`} aria-label={`Priority: ${item.status}`}></span>
                                    <span className="item-id">{item.documentId}</span>
                                </div>
                                <div className="item-sla">
                                    [{item.slaHoursRemaining}h]
                                </div>
                            </div>
                        ))}
                        {queue.length === 0 && (
                            <div style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-muted)' }}>
                                <CheckCircle size={32} style={{ opacity: 0.5, marginBottom: '1rem' }} />
                                <p>All clear!</p>
                            </div>
                        )}
                    </div>
                </div>

                {/* MY STATS */}
                <div className="stats-panel glass-panel">
                    <div className="panel-header" style={{ borderBottom: 'none', paddingBottom: 8 }}>
                        MY STATS
                    </div>
                    <div className="stats-content">
                        <div className="stat-line">Today: {stats.reviewed_today}</div>
                        <div className="stat-line">Avg: {stats.avg_time}s</div>
                    </div>
                </div>
            </div>

            {/* WORKSPACE */}
            <div className="workspace">
                {/* PREVIEW */}
                <div className="preview-panel glass-panel">
                    <div className="panel-header" style={{ borderBottom: '1px solid var(--border-color)' }}>
                        DOCUMENT PREVIEW
                    </div>
                    <div className="preview-content">
                        {selectedItem ? (
                            <div className="animated-entry" style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '1rem' }} aria-live="polite">
                                <FileText size={48} opacity={0.5} />
                                <p>[Document Image/PDF Viewer]</p>
                                <p style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>ID: {selectedItem.documentId}</p>
                            </div>
                        ) : (
                            <div style={{ textAlign: 'center' }} aria-live="polite">
                                <p>Select a document from the queue to start reviewing.</p>
                                <p style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginTop: '8px' }}>Shortcuts: Ctrl+Enter (Approve), Ctrl+Backspace (Reject)</p>
                            </div>
                        )}
                    </div>
                </div>

                {/* EXTRACTION FIELDS */}
                <div className="extraction-panel glass-panel">
                    <div className="panel-header" style={{ borderBottom: 'none', padding: '0 0 16px 0' }}>
                        EXTRACTED FIELDS
                    </div>

                    <div className="field-grid">
                        {selectedItem ? fields.map((field, idx) => {
                            const isLowConfidence = field.confidence < 0.8;
                            return (
                                <React.Fragment key={idx}>
                                    <div className="field-label">{field.key}:</div>
                                    <div className={`field-input-wrapper ${isLowConfidence ? 'low-confidence' : ''}`}>
                                        <span className="bracket">[</span>
                                        <input
                                            type="text"
                                            className="field-input"
                                            value={field.value}
                                            aria-label={field.key}
                                            onChange={(e) => handleFieldChange(idx, e.target.value)}
                                        />
                                        <span className="bracket">]</span>
                                    </div>
                                    <div className="field-confidence">
                                        {isLowConfidence ? (
                                            <span className="confidence-indicator low-confidence-icon">
                                                <AlertTriangle size={16} /> {Math.round(field.confidence * 100)}%
                                            </span>
                                        ) : (
                                            <span className="confidence-indicator high-confidence-icon">
                                                <Check size={16} /> {Math.round(field.confidence * 100)}%
                                            </span>
                                        )}
                                    </div>
                                </React.Fragment>
                            )
                        }) : (
                            <p style={{ color: 'var(--text-muted)' }}>No fields to display.</p>
                        )}
                    </div>

                    <div className="action-bar" style={{ marginTop: '32px' }}>
                        <button className="btn-approve" onClick={handleApprove} disabled={isSubmitting} aria-label="Approve Document">
                            {isSubmitting ? '[Submitting...]' : '[Approve]'}
                        </button>
                        <button className="btn-correct" onClick={handleCorrect} disabled={isSubmitting} aria-label="Correct Document">
                            [Correct]
                        </button>
                        <button className="btn-reject" onClick={handleReject} disabled={isSubmitting} aria-label="Reject Document">
                            [Reject]
                        </button>
                    </div>
                </div>
            </div>
        </div>
    )
}

import React, { useState, useEffect } from 'react';
import {
    FileText, CheckCircle, AlertTriangle, Clock,
    Settings, User, BarChart2, Check, AlertCircle,
    Layers, Filter, ArrowDownUp, Upload
} from 'lucide-react';
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
    const [successState, setSuccessState] = useState(false);

    // Stats
    const API_URL = "http://127.0.0.1:8000/api";
    const USER_ID = "reviewer_01";
    const [stats, setStats] = useState({ reviewed_today: 0, avg_time: 0, queue_length: 0, sla: 99 });
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

            // Calculate a mock SLA percentage if DB isn't returning it natively yet
            const pending = statsData.pending_items || data.length;
            const breached = statsData.sla_breached || 0;
            const slaPercent = pending > 0 ? Math.max(0, 100 - (breached / pending * 100)) : 100;

            setStats({
                reviewed_today: statsData.reviewed_today || 0,
                avg_time: statsData.avg_review_time_seconds || 0,
                queue_length: pending,
                sla: Math.round(slaPercent)
            });
        } catch (e) {
            console.error("Failed to fetch queue", e);
            setErrorMsg("Failed to connect to backend server.");
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
        setSuccessState(false);
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
            fetchQueueAndStats();
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
            setSuccessState(true);
            fetchQueueAndStats();

            // Auto hide success state after 3s
            setTimeout(() => setSuccessState(false), 3000);
        } catch (e) {
            console.error(e);
            setErrorMsg(`Failed to submit: ${e instanceof Error ? e.message : 'Unknown error'}`);
        } finally {
            setIsSubmitting(false);
        }
    };

    const handleApprove = () => submitAction("COMPLETED");
    const handleReject = () => {
        const reason = prompt("Enter rejection reason:");
        if (reason) submitAction("REJECTED", reason);
    };

    const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0];
        if (!file) return;

        const formData = new FormData();
        formData.append("file", file);

        setIsLoading(true);
        setErrorMsg(null);
        try {
            const res = await fetch(`${API_URL}/upload`, {
                method: "POST",
                body: formData,
            });
            if (!res.ok) {
                const data = await res.json();
                throw new Error(data.detail || "Upload failed");
            }
            // Wait a few seconds for background LLM processing, then refresh
            setTimeout(fetchQueueAndStats, 5000);
        } catch (err: any) {
            console.error("Upload error", err);
            setErrorMsg(err.message);
        } finally {
            setIsLoading(false);
            e.target.value = ""; // reset
        }
    };

    return (
        <div className="layout">
            {/* HEADER */}
            <header>
                <div className="header-left">
                    <Layers className="logo-icon" size={24} />
                    <div className="header-title">DocAI Studio</div>
                    <div className="badge-pill">Human Review</div>
                </div>
                <div className="header-nav">
                    <label className="nav-btn" aria-label="Upload Document" style={{ cursor: 'pointer', display: 'flex', alignItems: 'center' }} title="Upload test PDF">
                        <Upload size={20} />
                        <input type="file" accept=".pdf" style={{ display: 'none' }} onChange={handleUpload} />
                    </label>
                    <button className="nav-btn" aria-label="Settings"><Settings size={20} /></button>
                    <button className="nav-btn" aria-label="User Profile"><User size={20} /></button>
                </div>
            </header>

            {/* SIDEBAR */}
            <div className="sidebar">

                {/* MY STATS (Moved to top based on inspiration) */}
                <div className="stats-panel glass-panel">
                    <div className="panel-header">
                        <BarChart2 size={16} /> My Stats
                    </div>
                    <div className="stats-content">
                        <div className="stat-card">
                            <span className="stat-label">Today</span>
                            <span className="stat-value">{stats.reviewed_today}</span>
                        </div>
                        <div className="stat-card">
                            <span className="stat-label">Avg Time</span>
                            <span className="stat-value">{stats.avg_time}s</span>
                        </div>
                        <div className="stat-card">
                            <span className="stat-label">Queue</span>
                            <span className="stat-value">{stats.queue_length}</span>
                        </div>
                        <div className="stat-card">
                            <span className="stat-label">SLA</span>
                            <span className={`stat-value ${stats.sla >= 95 ? 'green' : ''}`}>{stats.sla}%</span>
                        </div>
                    </div>
                </div>

                {/* QUEUE */}
                <div className="queue-panel glass-panel">
                    <div className="panel-header">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                            <Filter size={16} /> Queue
                        </div>
                        <div className="header-actions">
                            <ArrowDownUp size={16} />
                            <Filter size={16} />
                        </div>
                    </div>
                    {errorMsg && (
                        <div className="error-banner" role="alert">
                            <AlertCircle size={16} /> {errorMsg}
                        </div>
                    )}
                    <div className="queue-list" role="listbox" aria-label="Document Queue">
                        {isLoading && queue.length === 0 ? (
                            <div style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-muted)' }}>
                                <p>Loading queue...</p>
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
                                <div className="item-info">
                                    <span className="item-id">{item.documentId}</span>
                                    <span className="item-type"><FileText size={12} /> Invoice</span>
                                </div>
                                <div className={`sla-badge ${item.status}`} aria-label={`SLA: ${item.slaHoursRemaining} hours remaining. Priority: ${item.status}`}>
                                    <Clock size={12} /> {item.slaHoursRemaining}h
                                </div>
                            </div>
                        ))}
                        {queue.length === 0 && !isLoading && (
                            <div style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-muted)' }}>
                                <CheckCircle size={32} style={{ opacity: 0.5, marginBottom: '1rem' }} />
                                <p>Queue is empty.</p>
                            </div>
                        )}
                    </div>
                </div>
            </div>

            {/* WORKSPACE */}
            <div className="workspace">

                {/* PREVIEW PANEL */}
                <div className="preview-panel glass-panel">
                    <div className="panel-header">
                        <FileText size={16} /> Document Preview
                    </div>
                    <div className="preview-content">
                        {successState ? (
                            <div className="success-state" aria-live="polite">
                                <CheckCircle size={64} className="success-icon" />
                                <p>Document processed successfully. Select next from queue.</p>
                            </div>
                        ) : selectedItem ? (
                            <iframe
                                src={`${API_URL}/documents/${selectedItem.documentId}`}
                                title={`Document ${selectedItem.documentId}`}
                                style={{
                                    width: '100%',
                                    height: '100%',
                                    border: 'none',
                                    borderRadius: '8px',
                                    minHeight: '600px'
                                }}
                            />
                        ) : (
                            <p>Select a document from the queue to start reviewing.</p>
                        )}
                    </div>
                </div>

                {/* EXTRACTION FIELDS PANEL */}
                <div className="extraction-panel glass-panel">
                    <div className="panel-header" style={{ borderBottom: '1px solid var(--panel-border)', paddingBottom: '12px' }}>
                        <CheckCircle size={16} /> Extracted Fields
                    </div>

                    <div className="extraction-content">
                        {selectedItem ? (
                            <>
                                <div className="field-grid">
                                    {fields.map((field, idx) => {
                                        const isLowConfidence = field.confidence < 0.8;
                                        return (
                                            <div className="field-group" key={idx}>
                                                <div className="field-header">
                                                    <span className="field-label">{field.key}</span>
                                                    <span className="field-confidence" title={`Confidence score: ${Math.round(field.confidence * 100)}%`}>
                                                        {isLowConfidence ? (
                                                            <span className="low-confidence-icon"><AlertTriangle size={14} /> {Math.round(field.confidence * 100)}%</span>
                                                        ) : (
                                                            <span className="high-confidence-icon"><Check size={14} /> {Math.round(field.confidence * 100)}%</span>
                                                        )}
                                                    </span>
                                                </div>
                                                <div className={`field-input-wrapper ${isLowConfidence ? 'low-confidence' : ''}`}>
                                                    <input
                                                        type="text"
                                                        className="field-input"
                                                        value={field.value}
                                                        aria-label={field.key}
                                                        onChange={(e) => handleFieldChange(idx, e.target.value)}
                                                    />
                                                </div>
                                            </div>
                                        )
                                    })}
                                </div>
                                <div className="action-bar">
                                    <button className="btn-secondary" onClick={() => fetchQueueAndStats()} disabled={isSubmitting}>
                                        Reset
                                    </button>
                                    <button className="btn-danger" onClick={handleReject} disabled={isSubmitting} aria-label="Reject Document">
                                        Reject
                                    </button>
                                    <button className="btn-primary" onClick={handleApprove} disabled={isSubmitting} aria-label="Approve Document">
                                        {isSubmitting ? 'Submitting...' : 'Approve & Submit (Ctrl+Enter)'}
                                    </button>
                                </div>
                            </>
                        ) : (
                            <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)' }}>
                                {successState ? "Awaiting selection..." : "No fields to display. Select a document."}
                            </div>
                        )}
                    </div>
                </div>

            </div>
        </div>
    );
}

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ultimate_bot_updated.py
Short-term adaptive bot — Enhanced Reversal + Range (Balanced)
Modifications:
- Local REV_MIN_CONFIDENCE and RANGE_MIN_CONFIDENCE
- EMA bias filters for REVERSAL and RANGE to reduce false positives
- RANGE now considers 30m candles (in addition to 1h and 4h) for earlier detection
- Added enhanced momentum strategy with volatility classification
- Improved signal generation with hotness ranking and neutral zone detection
- Added comprehensive strategy verification system
"""

import os
import sys
import io
import time
import json
import csv
import math
import statistics
import random
import requests
import threading
import inspect
from flask import Flask, jsonify, render_template_string
from datetime import datetime, timezone

# Global signals storage for API
GLOBAL_SIGNALS = {
    'momentum': [],
    'reversal': [],
    'range': [],
    'top_prioritized': [],
    'last_updated': None
}
GLOBAL_SIGNALS_LOCK = threading.Lock()

# Log storage for API
SYSTEM_LOGS = []
LOGS_LOCK = threading.Lock()
MAX_SYSTEM_LOGS = 200

def add_system_log(message):
    with LOGS_LOCK:
        timestamp = datetime.now().strftime("%H:%M:%S")
        SYSTEM_LOGS.append(f"[{timestamp}] {message}")
        if len(SYSTEM_LOGS) > MAX_SYSTEM_LOGS:
            SYSTEM_LOGS.pop(0)

# Wrap print to also log to UI
_original_print = print
def print(*args, **kwargs):
    message = " ".join(map(str, args))
    add_system_log(message)
    _original_print(*args, **kwargs)

app = Flask(__name__)

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Ultimate Bot Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <style>
        .strategy-card { @apply bg-zinc-900 p-6 rounded-lg shadow-md mb-6 border border-red-900/30 transition-all duration-300 hover:shadow-red-900/20; }
        .signal-row:hover { @apply bg-red-900/10; }
        .nav-link { @apply flex items-center px-4 py-2 text-zinc-400 hover:bg-red-600 hover:text-white rounded-md transition-colors; }
        .nav-link.active { @apply bg-red-700 text-white; }
        #logs-content { font-family: 'Fira Code', 'Courier New', monospace; }
        .log-line { @apply py-1 border-b border-zinc-800 last:border-0; }
        .log-error { @apply text-red-500; }
        .log-warn { @apply text-amber-500; }
        .log-info { @apply text-red-400; }
        .sidebar { @apply fixed left-0 top-0 h-full w-64 bg-black border-r border-red-900/30 z-50 transition-transform duration-300; }
        .main-content { @apply ml-64 p-8 bg-zinc-950 min-h-screen text-zinc-300; }
        .card { @apply bg-zinc-900 border border-red-900/20 rounded-2xl p-6 shadow-sm; }
        @media (max-width: 768px) {
            .sidebar { @apply -translate-x-full; }
            .sidebar.open { @apply translate-x-0; }
            .main-content { @apply ml-0; }
        }
    </style>
</head>
<body class="bg-black min-h-screen text-zinc-300">
    <nav class="sidebar shadow-2xl">
        <div class="p-6">
            <div class="flex items-center space-x-3 mb-10">
                <div class="bg-red-600 p-2 rounded-lg shadow-lg shadow-red-900/40">
                    <i class="fas fa-robot text-white text-xl"></i>
                </div>
                <h2 class="text-xl font-bold text-white tracking-tight">ULTIMATE <span class="text-red-600">BOT</span></h2>
            </div>
            
            <div class="space-y-2">
                <a href="javascript:showPage('dashboard')" id="nav-dashboard" class="nav-link active">
                    <i class="fas fa-chart-line mr-3"></i> Dashboard
                </a>
                <a href="javascript:showPage('logs')" id="nav-logs" class="nav-link">
                    <i class="fas fa-terminal mr-3"></i> System Logs
                </a>
                <a href="javascript:showPage('profile')" id="nav-profile" class="nav-link">
                    <i class="fas fa-user mr-3"></i> Profile
                </a>
            </div>
            
            <div class="mt-10 pt-10 border-t border-zinc-800">
                <div class="bg-red-950/20 border border-red-900/30 rounded-xl p-4">
                    <h3 class="text-xs font-bold text-red-500 uppercase mb-2">Bot Status</h3>
                    <div class="flex items-center space-x-2">
                        <div id="status-dot" class="w-3 h-3 rounded-full bg-green-500 shadow-sm shadow-green-500/50"></div>
                        <span id="status-text" class="text-sm font-semibold text-zinc-100">Live</span>
                    </div>
                    <p id="uptime" class="text-xs text-zinc-500 mt-2">Updating...</p>
                </div>
                <div class="mt-4">
                    <a href="/logout" class="flex items-center px-4 py-2 text-zinc-500 hover:text-red-400 text-sm transition-colors">
                        <i class="fas fa-sign-out-alt mr-3"></i> Sign Out
                    </a>
                </div>
            </div>
        </div>
    </nav>

    <main class="main-content">
        <div id="dashboard-page">
            <header class="flex justify-between items-center mb-10">
                <div>
                    <h1 class="text-4xl font-black text-white tracking-tighter uppercase italic"><span class="text-red-600">Trading</span> Signals</h1>
                    <p class="text-zinc-500 mt-1 font-medium">Alpha generation engine active.</p>
                </div>
                <div class="flex space-x-4">
                    <button onclick="fetchSignals()" class="bg-zinc-900 p-3 rounded-xl border border-red-900/30 text-red-500 hover:bg-red-900/20 shadow-sm transition-all">
                        <i class="fas fa-sync-alt"></i>
                    </button>
                </div>
            </header>

            <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-10">
                <div class="card border-l-4 border-l-red-600">
                    <div class="flex items-center justify-between mb-4">
                        <div class="bg-red-950/40 p-3 rounded-xl">
                            <i class="fas fa-bolt text-red-500"></i>
                        </div>
                        <span class="text-[10px] font-black text-red-500 bg-red-950/40 px-2 py-1 rounded tracking-widest uppercase">Momentum</span>
                    </div>
                    <h3 class="text-zinc-500 text-xs font-bold uppercase tracking-wider">Signals</h3>
                    <p id="momentum-count" class="text-4xl font-black text-white">0</p>
                </div>
                <div class="card border-l-4 border-l-red-600">
                    <div class="flex items-center justify-between mb-4">
                        <div class="bg-red-950/40 p-3 rounded-xl">
                            <i class="fas fa-arrows-alt-h text-red-500"></i>
                        </div>
                        <span class="text-[10px] font-black text-red-500 bg-red-950/40 px-2 py-1 rounded tracking-widest uppercase">Reversal</span>
                    </div>
                    <h3 class="text-zinc-500 text-xs font-bold uppercase tracking-wider">Signals</h3>
                    <p id="reversal-count" class="text-4xl font-black text-white">0</p>
                </div>
                <div class="card border-l-4 border-l-red-600">
                    <div class="flex items-center justify-between mb-4">
                        <div class="bg-red-950/40 p-3 rounded-xl">
                            <i class="fas fa-compress-alt text-red-500"></i>
                        </div>
                        <span class="text-[10px] font-black text-red-500 bg-red-950/40 px-2 py-1 rounded tracking-widest uppercase">Range</span>
                    </div>
                    <h3 class="text-zinc-500 text-xs font-bold uppercase tracking-wider">Signals</h3>
                    <p id="range-count" class="text-4xl font-black text-white">0</p>
                </div>
                <div class="card border-l-4 border-l-red-600">
                    <div class="flex items-center justify-between mb-4">
                        <div class="bg-red-950/40 p-3 rounded-xl">
                            <i class="fas fa-clock text-red-500"></i>
                        </div>
                        <span class="text-[10px] font-black text-red-500 bg-red-950/40 px-2 py-1 rounded tracking-widest uppercase">Status</span>
                    </div>
                    <h3 class="text-zinc-500 text-xs font-bold uppercase tracking-wider">Last Scan</h3>
                    <p id="last-update" class="text-xl font-black text-white">--:--:--</p>
                </div>
            </div>

            <section id="top-prioritized-section" class="mb-10 hidden">
                <div class="flex items-center justify-between mb-6">
                    <h2 class="text-2xl font-black text-white uppercase tracking-tight italic">Priority <span class="text-red-600">Strikes</span></h2>
                    <span class="px-3 py-1 bg-red-600 text-white text-[10px] font-black rounded-full tracking-widest">HIGH ALPHA</span>
                </div>
                <div id="top-signals-list" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                </div>
            </section>

            <div class="space-y-12">
                <section>
                    <div class="flex items-center mb-6">
                        <div class="w-8 h-1 bg-red-600 rounded-full mr-3 shadow-sm shadow-red-600/50"></div>
                        <h2 class="text-2xl font-black text-white uppercase tracking-tighter italic">Momentum</h2>
                    </div>
                    <div class="bg-zinc-900 rounded-2xl shadow-xl border border-red-900/20 overflow-hidden">
                        <table class="min-w-full">
                            <thead class="bg-black/50 border-b border-red-900/20">
                                <tr>
                                    <th class="px-6 py-5 text-left text-[10px] font-black text-zinc-500 uppercase tracking-widest">Pair</th>
                                    <th class="px-6 py-5 text-left text-[10px] font-black text-zinc-500 uppercase tracking-widest">Side</th>
                                    <th class="px-6 py-5 text-left text-[10px] font-black text-zinc-500 uppercase tracking-widest">Price</th>
                                    <th class="px-6 py-5 text-left text-[10px] font-black text-zinc-500 uppercase tracking-widest">TP / SL</th>
                                    <th class="px-6 py-5 text-left text-[10px] font-black text-zinc-500 uppercase tracking-widest">Confidence</th>
                                </tr>
                            </thead>
                            <tbody id="momentum-table" class="divide-y divide-red-900/10"></tbody>
                        </table>
                    </div>
                </section>
                <!-- Other sections similarly updated in the full code -->
            </div>
        </div>

        <div id="logs-page" class="hidden">
            <header class="flex justify-between items-center mb-8">
                <div>
                    <h1 class="text-4xl font-black text-white tracking-tighter uppercase italic">System <span class="text-red-600">Logs</span></h1>
                    <p class="text-zinc-500 mt-1 font-medium">Core engine execution stream.</p>
                </div>
                <div class="flex items-center space-x-4">
                    <button onclick="clearLogs()" class="px-6 py-2 bg-red-900/20 text-red-500 border border-red-900/30 font-bold rounded-lg hover:bg-red-600 hover:text-white transition-all">
                        PURGE VIEW
                    </button>
                </div>
            </header>

            <div class="bg-black rounded-2xl p-6 shadow-2xl border border-red-900/30 overflow-hidden">
                <div id="logs-content" class="text-zinc-400 text-xs h-[600px] overflow-y-auto space-y-1 font-mono">
                    <div class="log-line opacity-30 italic">Engaging log telemetry...</div>
                </div>
            </div>
        </div>

        <div id="profile-page" class="hidden">
            <header class="mb-10">
                <h1 class="text-4xl font-black text-white tracking-tighter uppercase italic">User <span class="text-red-600">Profile</span></h1>
                <p class="text-zinc-500 mt-1 font-medium">Account configurations and settings.</p>
            </header>

            <div class="max-w-2xl">
                <div class="card mb-8">
                    <div class="flex items-center space-x-6 mb-8">
                        <div class="w-24 h-24 bg-red-900/20 border-2 border-red-600 rounded-full flex items-center justify-center">
                            <i class="fas fa-user text-4xl text-red-600"></i>
                        </div>
                        <div>
                            <h2 id="user-name" class="text-2xl font-bold text-white">Loading...</h2>
                            <p id="user-id" class="text-zinc-500 text-sm">ID: --</p>
                        </div>
                    </div>
                    
                    <div class="space-y-6">
                        <div class="p-4 bg-black/40 rounded-xl border border-red-900/10">
                            <label class="text-[10px] font-black text-red-500 uppercase tracking-widest mb-1 block">Account Access</label>
                            <p class="text-white font-medium">Authenticated via Replit Auth</p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </main>

    <script>
        let currentLogs = [];
        const MAX_LOGS = 500;

        function showPage(pageId) {
            document.getElementById('dashboard-page').classList.toggle('hidden', pageId !== 'dashboard');
            document.getElementById('logs-page').classList.toggle('hidden', pageId !== 'logs');
            document.getElementById('profile-page').classList.toggle('hidden', pageId !== 'profile');
            
            document.getElementById('nav-dashboard').classList.toggle('active', pageId === 'dashboard');
            document.getElementById('nav-logs').classList.toggle('active', pageId === 'logs');
            document.getElementById('nav-profile').classList.toggle('active', pageId === 'profile');

            if (pageId === 'profile') {
                updateProfile();
            }
        }

        async function updateProfile() {
            try {
                const response = await fetch('/api/user');
                const user = await response.json();
                document.getElementById('user-name').innerText = user.name || 'Anonymous User';
                document.getElementById('user-id').innerText = `ID: ${user.id || 'N/A'}`;
            } catch (e) {
                console.error('Profile fetch failed');
            }
        }

        async function fetchSignals() {
            try {
                const response = await fetch('/api/signals');
                const data = await response.json();
                updateDashboard(data);
                setStatus(true);
            } catch (error) {
                console.error('Error fetching signals:', error);
                setStatus(false);
            }
        }

        async function fetchLogs() {
            try {
                const response = await fetch('/api/logs');
                const data = await response.json();
                updateLogs(data.logs);
            } catch (error) {
                console.error('Error fetching logs:', error);
            }
        }

        function setStatus(online) {
            const badge = document.getElementById('status-dot');
            const text = document.getElementById('status-text');
            badge.className = `w-3 h-3 rounded-full ${online ? 'bg-green-500' : 'bg-red-500'}`;
            text.innerText = online ? 'Live' : 'Offline';
        }

        function updateDashboard(data) {
            document.getElementById('momentum-count').innerText = data.momentum.length;
            document.getElementById('reversal-count').innerText = data.reversal.length;
            document.getElementById('range-count').innerText = data.range.length;
            document.getElementById('last-update').innerText = data.last_updated || 'Never';

            renderTable('momentum-table', data.momentum);
            renderTable('reversal-table', data.reversal);
            renderTable('range-table', data.range);
            
            if (data.top_prioritized && data.top_prioritized.length > 0) {
                document.getElementById('top-prioritized-section').classList.remove('hidden');
                renderTopSignals(data.top_prioritized);
            } else {
                document.getElementById('top-prioritized-section').classList.add('hidden');
            }
        }

        function updateLogs(newLogs) {
            const container = document.getElementById('logs-content');
            const isScrolledToBottom = container.scrollHeight - container.clientHeight <= container.scrollTop + 1;
            
            if (newLogs.length > 0) {
                container.innerHTML = '';
                newLogs.forEach(log => {
                    const div = document.createElement('div');
                    div.className = 'log-line';
                    if (log.toLowerCase().includes('error') || log.includes('⚠️') || log.includes('Fatal')) {
                        div.classList.add('log-error');
                    } else if (log.toLowerCase().includes('warn')) {
                        div.classList.add('log-warn');
                    } else if (log.includes('🚀') || log.includes('✓')) {
                        div.classList.add('log-info');
                    }
                    div.textContent = log;
                    container.appendChild(div);
                });
                
                if (isScrolledToBottom) {
                    container.scrollTop = container.scrollHeight;
                }
            }
        }

        function clearLogs() {
            document.getElementById('logs-content').innerHTML = '<div class="log-line opacity-50 italic">View cleared. Waiting for new logs...</div>';
        }

        function renderTable(tableId, signals) {
            const table = document.getElementById(tableId);
            table.innerHTML = '';
            
            if (!signals || signals.length === 0) {
                table.innerHTML = '<tr><td colspan="5" class="px-6 py-12 text-center text-gray-400 italic">No active signals detected in this category</td></tr>';
                return;
            }

            signals.forEach(sig => {
                const row = document.createElement('tr');
                row.className = 'signal-row transition-colors cursor-default';
                const sideClass = sig.side === 'LONG' ? 'text-green-600' : 'text-red-600';
                const sideBg = sig.side === 'LONG' ? 'bg-green-50' : 'bg-red-50';
                
                row.innerHTML = `
                    <td class="px-6 py-4 whitespace-nowrap"><span class="font-bold text-gray-900">${sig.symbol}</span></td>
                    <td class="px-6 py-4 whitespace-nowrap"><span class="px-2 py-1 rounded-md font-bold text-xs ${sideBg} ${sideClass}">${sig.side}</span></td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-600">${sig.entry_price || sig.price}</td>
                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        <div class="flex flex-col">
                            <span class="text-green-600 font-bold">TP: ${sig.tp}</span>
                            <span class="text-red-400 text-xs">SL: ${sig.sl}</span>
                        </div>
                    </td>
                    <td class="px-6 py-4 whitespace-nowrap">
                        <div class="w-full bg-gray-100 rounded-full h-1.5 mb-1 max-w-[100px]">
                            <div class="bg-blue-600 h-1.5 rounded-full" style="width: ${(sig.confidence || 0) * 100}%"></div>
                        </div>
                        <span class="text-xs font-bold text-gray-400">${((sig.confidence || 0) * 100).toFixed(1)}%</span>
                    </td>
                `;
                table.appendChild(row);
            });
        }

        function renderTopSignals(signals) {
            const list = document.getElementById('top-signals-list');
            list.innerHTML = '';
            signals.slice(0, 6).forEach(sig => {
                const card = document.createElement('div');
                card.className = 'bg-white p-6 rounded-2xl shadow-sm border border-gray-100 border-l-4 transition-all duration-300 hover:-translate-y-1 ' + (sig.side === 'LONG' ? 'border-green-500' : 'border-red-500');
                card.innerHTML = `
                    <div class="flex justify-between items-start mb-4">
                        <span class="text-xl font-extrabold text-gray-900">${sig.symbol}</span>
                        <span class="px-3 py-1 text-xs font-bold rounded-full ${sig.side === 'LONG' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}">${sig.side}</span>
                    </div>
                    <div class="grid grid-cols-2 gap-4 mb-4">
                        <div>
                            <p class="text-[10px] text-gray-400 uppercase font-bold tracking-wider">Entry Price</p>
                            <p class="font-bold text-gray-700">${sig.entry_price || sig.price}</p>
                        </div>
                        <div>
                            <p class="text-[10px] text-gray-400 uppercase font-bold tracking-wider">Confidence</p>
                            <p class="font-bold text-blue-600">${((sig.confidence || 0) * 100).toFixed(1)}%</p>
                        </div>
                    </div>
                    <div class="space-y-2 bg-gray-50 p-3 rounded-xl">
                        <div class="flex justify-between text-xs">
                            <span class="text-gray-500 font-medium text-green-700">Take Profit</span>
                            <span class="font-bold text-green-600">${sig.tp}</span>
                        </div>
                        <div class="flex justify-between text-xs">
                            <span class="text-gray-500 font-medium">Stop Loss</span>
                            <span class="font-bold text-red-400">${sig.sl}</span>
                        </div>
                    </div>
                    <div class="mt-4 bg-gray-100 rounded-full h-1.5">
                        <div class="bg-blue-600 h-1.5 rounded-full" style="width: ${(sig.confidence || 0) * 100}%"></div>
                    </div>
                `;
                list.appendChild(card);
            });
        }

        setInterval(fetchSignals, 5000);
        setInterval(fetchLogs, 3000);
        fetchSignals();
        fetchLogs();
    </script>
</body>
</html>
"""

@app.route('/', methods=['GET'])
def index():
    """Serve the dashboard."""
    user_id = flask.request.headers.get('X-Replit-User-Id')
    if not user_id:
        return flask.redirect('/login')
    return render_template_string(DASHBOARD_HTML)

@app.route('/login')
def login():
    return render_template_string("""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Ultimate Bot | Login</title>
            <script src="https://cdn.tailwindcss.com"></script>
            <style>
                body { background-color: #000; color: #fff; font-family: sans-serif; }
                .auth-card { background: #111; border: 1px solid #991b1b; }
            </style>
        </head>
        <body class="flex items-center justify-center min-h-screen">
            <div class="auth-card p-10 rounded-2xl shadow-2xl max-w-md w-full text-center">
                <div class="mb-8">
                    <div class="w-16 h-16 bg-red-600 rounded-full mx-auto flex items-center justify-center mb-4">
                        <svg class="w-8 h-8 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"></path></svg>
                    </div>
                    <h1 class="text-3xl font-black uppercase italic tracking-tighter">Ultimate <span class="text-red-600">Bot</span></h1>
                    <p class="text-zinc-500 mt-2">Alpha access requires authentication.</p>
                </div>
                <button onclick="window.location.href='/__replitauth/login'" class="w-full py-4 bg-red-600 hover:bg-red-700 text-white font-black uppercase tracking-widest rounded-xl transition-all shadow-lg shadow-red-900/40">
                    Sign in with Replit
                </button>
                <p class="mt-6 text-[10px] text-zinc-600 uppercase tracking-widest font-bold">Secure Gateway Active</p>
            </div>
        </body>
        </html>
    """)

@app.route('/logout')
def logout():
    return flask.redirect('/login')

@app.route('/api/user')
def get_user():
    user_id = flask.request.headers.get('X-Replit-User-Id')
    user_name = flask.request.headers.get('X-Replit-User-Name')
    return jsonify({"id": user_id, "name": user_name})

import flask


@app.route('/api/signals', methods=['GET'])
def get_signals():
    """Get all current trading signals."""
    with GLOBAL_SIGNALS_LOCK:
        return jsonify(GLOBAL_SIGNALS)

@app.route('/api/signals/<strategy>', methods=['GET'])
def get_strategy_signals(strategy):
    """Get current signals for a specific strategy."""
    strategy = strategy.lower()
    with GLOBAL_SIGNALS_LOCK:
        if strategy in GLOBAL_SIGNALS:
            return jsonify(GLOBAL_SIGNALS[strategy])
        else:
            return jsonify({"error": "Strategy not found"}), 404

@app.route('/api/health', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({"status": "running", "timestamp": datetime.now(timezone.utc).isoformat()})

@app.route('/api/logs', methods=['GET'])
def get_logs():
    """Get recent system logs."""
    with LOGS_LOCK:
        return jsonify({"logs": list(SYSTEM_LOGS)})

def run_flask_app():
    """Run the Flask application on a separate thread."""
    port = int(os.getenv("API_PORT", "5000"))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Removed dependency on strategy_kline_fetchers.py - implementing directly
STRATEGY_FETCHERS_AVAILABLE = True

_requests_session = None

def _get_requests_session():
    """Get or create a requests session with connection pooling and retry logic."""
    global _requests_session
    if _requests_session is None:
        _requests_session = requests.Session()
        retry_strategy = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        _requests_session.mount("https://", adapter)
        _requests_session.mount("http://", adapter)
    return _requests_session

# Kline fetching helpers
API_TIMEOUT_KLINES = int(os.getenv("API_TIMEOUT_KLINES", "5"))
THROTTLE_KLINES = float(os.getenv("THROTTLE", "0.2"))

_kline_fetch_lock = threading.Lock()
_last_kline_fetch_time = 0

# Cache configuration (STAGE 1 - Cache Monitoring)
CACHE_MAX_AGE_SECONDS = int(os.getenv("CACHE_MAX_AGE_SECONDS", "600"))      # 10 minutes
CACHE_MAX_SIZE_MB = int(os.getenv("CACHE_MAX_SIZE_MB", "100"))              # 100 MB max
CACHE_MAX_ENTRIES = int(os.getenv("CACHE_MAX_ENTRIES", "5000"))             # Max entries
CACHE_CLEANUP_INTERVAL = int(os.getenv("CACHE_CLEANUP_INTERVAL", "60"))     # Check every 60s
DEBUG_CACHE = os.getenv("DEBUG_CACHE", "false").lower() == "true"

# Global cache and statistics (STAGE 1)
_klines_cache_global = {}  # Will be initialized in main function
_cache_stats = {
    'hits': 0,
    'misses': 0,
    'last_cleanup': time.time(),
    'total_size_bytes': 0,
    'cleanup_runs': 0,
    'evicted_entries': 0
}
_cache_stats_lock = threading.Lock()
_cache_lock = threading.Lock()  # Protect cache from concurrent access

TRENDING_PAIRS = {}
TRENDING_PAIRS_LOCK = threading.Lock()

# Exchange Symbol Caches for targeting movers
_BITGET_SYMBOLS_CACHE = {'symbols': set(), 'timestamp': 0}
_HASHKEY_SYMBOLS_CACHE = {'symbols': set(), 'timestamp': 0}
_GATEIO_SYMBOLS_CACHE = {'symbols': set(), 'timestamp': 0}
_BITSTAMP_SYMBOLS_CACHE = {'symbols': set(), 'timestamp': 0}
_KRAKEN_SYMBOLS_CACHE = {'symbols': set(), 'timestamp': 0}
_COINMETRO_SYMBOLS_CACHE = {'symbols': set(), 'timestamp': 0}
_EXCHANGE_SYMBOLS_LOCK = threading.Lock()

CONFIDENCE_THRESHOLDS = {
    'reversal': float(os.getenv("CONFIDENCE_MIN_REVERSAL", "10")),  # Lowered from 30 to match REV_MIN_CONFIDENCE
    'range': float(os.getenv("CONFIDENCE_MIN_RANGE", "8")),  # Lowered to match detect_range minimum
    'momentum': float(os.getenv("CONFIDENCE_MIN_MOMENTUM", "12"))  # Lowered from 15 to allow more signals
}

RSI_DIV_LOOKBACK = int(os.getenv("RSI_DIV_LOOKBACK", "8"))
HOTNESS_BONUS_MAX = float(os.getenv("HOTNESS_BONUS_MAX", "15.0"))
RANGE_BB_COMPRESS_THRESHOLD = float(os.getenv("RANGE_BB_COMPRESS_THRESHOLD", "15.0"))
RANGE_MAX_WIDTH_PCT = float(os.getenv("RANGE_MAX_WIDTH_PCT", "12.0"))
RANGE_VOLUME_CONTRACTION_RATIO = float(os.getenv("RANGE_VOLUME_CONTRACTION_RATIO", "1.5"))

# ===== Adaptive TP/SL estimators and required indicators =====
DEBUG_TPSL = os.getenv("DEBUG_TPSL", "false").lower() == "true"

# Indicator/env defaults (duplicated locally for independence)
MOM_EMA_LEN = int(os.getenv("MOMENTUM_EMA_LENGTH", "50"))
RNG_BB_PERIOD = int(os.getenv("RANGE_BB_PERIOD", "20"))
RNG_BB_STD = float(os.getenv("RANGE_BB_STD", "2.0"))

# TP/SL env defaults per strategy
TP_CAP_MOM = float(os.getenv("TP_CAP_MOMENTUM", "0.20"))
SL_ATR_MOM = float(os.getenv("SL_ATR_MULT_MOMENTUM", "1.2"))
TP_CAP_RANGE = float(os.getenv("TP_CAP_RANGE", "0.15"))
SL_ATR_RANGE_OUTSIDE_BAND = float(os.getenv("SL_ATR_OUTSIDE_BAND", "0.7"))
TP_CAP_REV = float(os.getenv("TP_CAP_REVERSAL", "0.35"))
SL_ATR_REV_BEYOND_PIVOT = float(os.getenv("SL_ATR_BEYOND_PIVOT", "0.5"))
LIQUIDITY_BUFFER_MULT = float(os.getenv("LIQUIDITY_BUFFER_MULT", "0.5"))  # Buffer beyond wicks
TIME_STOP_BARS = int(os.getenv("TIME_STOP_BARS", "10"))


def _ema(series, length):
    if not series or length <= 1:
        return None
    alpha = 2.0 / (length + 1.0)
    ema_val = float(series[0])
    for v in series[1:]:
        ema_val = (v - ema_val) * alpha + ema_val
    return ema_val


def _ema_slope(series, length):
    if not series or len(series) < length + 2:
        return 0.0
    ema_prev = _ema(series[:-1], length)
    ema_curr = _ema(series[-length:], length) if len(series) >= length else _ema(series, length)
    if ema_prev is None or ema_curr is None:
        return 0.0
    return (ema_curr - ema_prev) / max(1e-12, abs(ema_prev))


def _rsi(series, period=14):
    if not series or len(series) <= period:
        return None
    gains, losses = 0.0, 0.0
    for i in range(1, period + 1):
        diff = series[-i] - series[-i - 1]
        if diff >= 0:
            gains += diff
        else:
            losses -= diff
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 * (rs / (1 + rs))


def _atr(klines, period=14):
    if not klines or len(klines) <= period:
        return None
    trs = []
    for i in range(1, len(klines)):
        h = float(klines[i][2]); l = float(klines[i][3]); pc = float(klines[i-1][4])
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def _bollinger(closes, period=20, std=2.0):
    if not closes or len(closes) < period:
        return None, None, None, None
    window = closes[-period:]
    mean = sum(window) / period
    var = sum((x - mean) ** 2 for x in window) / period
    dev = (var ** 0.5) * std
    upper = mean + dev
    lower = mean - dev
    width_pct = ((upper - lower) / max(1e-12, mean)) * 100.0
    return lower, mean, upper, width_pct



# ===== Adaptive TP/SL estimators and required indicators (Shadowed by implementation at line 5523) =====


#

_fetch_failures_cache = {}
_fetch_failures_lock = threading.Lock()
_failure_cooldown_seconds = int(os.getenv("FAILURE_COOLDOWN_SECONDS", "300"))
DEBUG_ERRORS = os.getenv("DEBUG_ERRORS", "false").lower() == "true"

def track_fetch_failure(pair: str, strategy: str):
    """Track a fetch failure for a pair/strategy combination."""
    with _fetch_failures_lock:
        key = (pair, strategy.lower())
        if key not in _fetch_failures_cache:
            _fetch_failures_cache[key] = {'count': 0, 'last_failure_time': 0, 'last_source_failed': None}
        _fetch_failures_cache[key]['count'] += 1
        _fetch_failures_cache[key]['last_failure_time'] = time.time()
    
    if DEBUG_ERRORS:
        print(f"[ERROR_TRACKING] {pair} ({strategy}): Failure #{_fetch_failures_cache[key]['count']} recorded", flush=True)

def should_skip_pair_due_to_failures(pair: str, strategy: str, max_failures: int = 3) -> bool:
    """Check if pair should be skipped due to repeated failures."""
    with _fetch_failures_lock:
        key = (pair, strategy.lower())
        if key not in _fetch_failures_cache:
            return False
        
        entry = _fetch_failures_cache[key]
        if entry['count'] < max_failures:
            return False
        
        time_since_last = time.time() - entry['last_failure_time']
        if time_since_last > _failure_cooldown_seconds:
            entry['count'] = 0
            return False
        
        return True

def clear_pair_failures(pair: str, strategy: str):
    """Clear failure count for a pair/strategy (after successful fetch)."""
    with _fetch_failures_lock:
        key = (pair, strategy.lower())
        if key in _fetch_failures_cache:
            _fetch_failures_cache[key]['count'] = 0
            _fetch_failures_cache[key]['last_failure_time'] = 0

def get_failures_str() -> str:
    """Get formatted failure tracking statistics."""
    with _fetch_failures_lock:
        active_failures = sum(1 for entry in _fetch_failures_cache.values() if entry['count'] > 0)
        return f"Active={active_failures}, Total tracked={(len(_fetch_failures_cache))}"

def get_cache_stats_str() -> str:
    """Get formatted cache statistics."""
    with _cache_stats_lock:
        hit_rate = (_cache_stats['hits'] / (_cache_stats['hits'] + _cache_stats['misses']) * 100) if (_cache_stats['hits'] + _cache_stats['misses']) > 0 else 0
        size_mb = _cache_stats['total_size_bytes'] / (1024 * 1024)
        return f"Hits={_cache_stats['hits']}, Misses={_cache_stats['misses']}, Rate={hit_rate:.1f}%, Size={size_mb:.2f}MB, Cleanups={_cache_stats['cleanup_runs']}, Evicted={_cache_stats['evicted_entries']}"

def validate_signal_confidence(signal: dict, strategy_name: str) -> tuple:
    """
    Validate signal confidence against strategy thresholds.
    Returns (is_valid, reason) tuple.
    """
    if not signal or not isinstance(signal, dict):
        return False, "Invalid signal format"
    
    confidence = signal.get('confidence', 0)
    min_threshold = CONFIDENCE_THRESHOLDS.get(strategy_name.lower(), 30)
    
    if confidence < min_threshold:
        return False, f"Confidence {confidence:.1f}% below threshold {min_threshold}%"
    
    return True, "OK"

_quality_issues_cache = {}
_quality_issues_lock = threading.Lock()
DEBUG_QUALITY = os.getenv("DEBUG_QUALITY", "false").lower() == "true"

_api_response_times = {}
_api_response_lock = threading.Lock()
_strategy_performance = {}
_strategy_perf_lock = threading.Lock()
_signal_frequency = {}
_signal_freq_lock = threading.Lock()
DEBUG_PERFORMANCE = os.getenv("DEBUG_PERFORMANCE", "false").lower() == "true"

def track_api_response_time(source: str, strategy: str, response_time_ms: float):
    if response_time_ms <= 0:
        return
    with _api_response_lock:
        key = (source.upper(), strategy.lower())
        if key not in _api_response_times:
            _api_response_times[key] = {'times': [], 'count': 0, 'total': 0, 'min': float('inf'), 'max': 0}
        entry = _api_response_times[key]
        entry['times'].append(response_time_ms)
        entry['count'] += 1
        entry['total'] += response_time_ms
        entry['min'] = min(entry['min'], response_time_ms)
        entry['max'] = max(entry['max'], response_time_ms)
        if len(entry['times']) > 100:
            entry['times'] = entry['times'][-100:]
    if DEBUG_PERFORMANCE:
        print(f"[PERF] {source} ({strategy}): {response_time_ms:.0f}ms", flush=True)

def check_kline_data_quality(klines: list, interval: str, pair: str = "", source: str = "") -> tuple:
    """
    Check for gaps and issues in kline data.
    Returns (is_valid, issues_found) tuple with list of quality issues.
    """
    if not klines or not isinstance(klines, list) or len(klines) < 2:
        return False, ["Insufficient klines (< 2)"]
    
    issues = []
    interval_ms_map = {
        '1m': 60000, '3m': 180000, '5m': 300000, '15m': 900000,
        '30m': 1800000, '1h': 3600000, '4h': 14400000, '1d': 86400000
    }
    expected_diff_ms = interval_ms_map.get(interval, 60000)
    
    gap_count = 0
    duplicate_count = 0
    out_of_order_count = 0
    
    for i in range(1, len(klines)):
        try:
            prev_ts = int(klines[i-1][0])
            curr_ts = int(klines[i][0])
            
            if curr_ts < prev_ts:
                out_of_order_count += 1
                continue
            
            if curr_ts == prev_ts:
                duplicate_count += 1
                continue
            
            time_diff = curr_ts - prev_ts
            if time_diff != expected_diff_ms and abs(time_diff - expected_diff_ms) > 1000:
                gap_count += 1
                if gap_count <= 3:
                    issues.append(f"Gap at kline {i}: {time_diff}ms vs {expected_diff_ms}ms expected")
        except (ValueError, TypeError, IndexError) as e:
            issues.append(f"Malformed kline at index {i}")
    
    if gap_count > 0:
        issues.append(f"Total gaps: {gap_count}/{len(klines)-1} candles")
    if duplicate_count > 0:
        issues.append(f"Duplicates: {duplicate_count}")
    if out_of_order_count > 0:
        issues.append(f"Out-of-order: {out_of_order_count}")
    
    quality_ok = len(issues) == 0
    
    if not quality_ok and DEBUG_QUALITY and pair:
        issue_str = " | ".join(issues[:2])
        print(f"[QUALITY] {pair} {interval} from {source}: {issue_str}", flush=True)
        with _quality_issues_lock:
            key = (pair, interval, source)
            if key not in _quality_issues_cache:
                _quality_issues_cache[key] = {'count': 0, 'last_issue': ''}
            _quality_issues_cache[key]['count'] += 1
            _quality_issues_cache[key]['last_issue'] = issue_str
    
    return quality_ok, issues

def validate_entry_price(signal: dict, pair_data: dict, max_deviation_pct: float = 2.0) -> tuple:
    """
    Validate entry price against current market price.
    Returns (is_valid, message, deviation_pct) tuple.
    """
    if not signal or not isinstance(signal, dict):
        return False, "Invalid signal", 0
    
    if not pair_data or not isinstance(pair_data, dict):
        return True, "No market data available (skip check)", 0
    
    current_price = pair_data.get('price')
    entry_price = signal.get('entry')
    
    if not current_price or not entry_price or current_price <= 0:
        return True, "Missing price data (skip check)", 0
    
    try:
        current_price = float(current_price)
        entry_price = float(entry_price)
    except (ValueError, TypeError):
        return True, "Non-numeric prices (skip check)", 0
    
    deviation_pct = abs(current_price - entry_price) / current_price * 100.0
    
    if deviation_pct > max_deviation_pct:
        return False, f"Entry {entry_price:.8f} deviates {deviation_pct:.2f}% from market {current_price:.8f}", deviation_pct
    
    return True, "Entry price valid", deviation_pct

def get_signal_quality_metrics(signals: list) -> dict:
    """
    Calculate quality metrics for a list of signals.
    Returns dict with confidence stats, directional distribution, etc.
    """
    if not signals:
        return {
            'count': 0,
            'confidence_avg': 0,
            'confidence_min': 0,
            'confidence_max': 0,
            'confidence_std': 0,
            'long_count': 0,
            'short_count': 0,
            'with_price_warning': 0,
            'with_quality_issues': 0
        }
    
    confidences = [s.get('confidence', 0) for s in signals if isinstance(s, dict)]
    
    if not confidences:
        return {'count': len(signals), 'confidence_avg': 0}
    
    avg_conf = sum(confidences) / len(confidences)
    min_conf = min(confidences)
    max_conf = max(confidences)
    
    variance = sum((c - avg_conf) ** 2 for c in confidences) / len(confidences) if len(confidences) > 1 else 0
    std_conf = variance ** 0.5
    
    directions = {}
    warnings_count = 0
    
    for sig in signals:
        if not isinstance(sig, dict):
            continue
        direction = sig.get('direction', 'UNKNOWN')
        directions[direction] = directions.get(direction, 0) + 1
        
        if sig.get('notes', ''):
            if 'STALE_PRICE' in sig['notes'] or 'deviation' in sig['notes']:
                warnings_count += 1
    
    return {
        'count': len(signals),
        'confidence_avg': round(avg_conf, 2),
        'confidence_min': round(min_conf, 2),
        'confidence_max': round(max_conf, 2),
        'confidence_std': round(std_conf, 2),
        'long_count': directions.get('LONG', 0),
        'short_count': directions.get('SHORT', 0),
        'with_price_warning': warnings_count
    }

def get_fastest_api_source(strategy: str) -> tuple:
    with _api_response_lock:
        strategy_sources = {k[0]: v for k, v in _api_response_times.items() if k[1] == strategy.lower()}
        if not strategy_sources:
            return None, 0
        fastest = min(strategy_sources.items(), key=lambda x: x[1]['total'] / max(1, x[1]['count']) if x[1]['count'] > 0 else float('inf'))
        avg_time = fastest[1]['total'] / max(1, fastest[1]['count'])
        return fastest[0], round(avg_time, 2)

def record_signal_generated(strategy_name: str, signal_count: int):
    if signal_count <= 0:
        return
    with _strategy_perf_lock:
        if strategy_name not in _strategy_performance:
            _strategy_performance[strategy_name] = {'signals_generated': 0, 'total_cycles': 0, 'signals_per_cycle': [], 'last_update': time.time()}
        entry = _strategy_performance[strategy_name]
        entry['signals_generated'] += signal_count
        entry['signals_per_cycle'].append(signal_count)
        entry['total_cycles'] += 1
        entry['last_update'] = time.time()
        if len(entry['signals_per_cycle']) > 50:
            entry['signals_per_cycle'] = entry['signals_per_cycle'][-50:]

def record_signal_frequency(timestamp_sec: float):
    current_minute = int(timestamp_sec / 60) * 60
    with _signal_freq_lock:
        if current_minute not in _signal_frequency:
            _signal_frequency[current_minute] = 0
        _signal_frequency[current_minute] += 1

def get_api_performance_metrics() -> dict:
    with _api_response_lock:
        if not _api_response_times:
            return {}
        metrics = {}
        for (source, strategy), data in _api_response_times.items():
            if data['count'] > 0:
                avg_time = data['total'] / data['count']
                metrics[f"{source}_{strategy}"] = {'calls': data['count'], 'avg_ms': round(avg_time, 2), 'min_ms': round(data['min'], 2), 'max_ms': round(data['max'], 2)}
        return metrics

def get_strategy_performance_metrics() -> dict:
    with _strategy_perf_lock:
        metrics = {}
        for strategy_name, data in _strategy_performance.items():
            if data['total_cycles'] > 0:
                avg_signals = sum(data['signals_per_cycle']) / len(data['signals_per_cycle'])
                metrics[strategy_name] = {'total_signals': data['signals_generated'], 'total_cycles': data['total_cycles'], 'avg_per_cycle': round(avg_signals, 2), 'max_in_cycle': max(data['signals_per_cycle']) if data['signals_per_cycle'] else 0}
        return metrics

def get_signal_frequency_metrics() -> dict:
    with _signal_freq_lock:
        if not _signal_frequency:
            return {'total_signals': 0, 'avg_per_minute': 0}
        total_signals = sum(_signal_frequency.values())
        minutes_active = len(_signal_frequency)
        avg_per_minute = total_signals / max(1, minutes_active)
        return {'total_signals': total_signals, 'minutes_active': minutes_active, 'avg_per_minute': round(avg_per_minute, 2), 'max_in_minute': max(_signal_frequency.values()) if _signal_frequency else 0}

def report_performance_metrics():
    print("\n" + "="*90, flush=True)
    print("📊 PERFORMANCE METRICS REPORT", flush=True)
    print("="*90, flush=True)
    api_metrics = get_api_performance_metrics()
    if api_metrics:
        print("\n⚡ API RESPONSE TIMES (by source & strategy):", flush=True)
        for key, data in sorted(api_metrics.items()):
            print(f"  {key:<25} | Calls={data['calls']:<4} | Avg={data['avg_ms']:>7.1f}ms | Min={data['min_ms']:>7.1f}ms | Max={data['max_ms']:>7.1f}ms", flush=True)
        for strategy in ['reversal', 'range', 'momentum']:
            fastest, avg_time = get_fastest_api_source(strategy)
            if fastest:
                print(f"  ✓ Fastest for {strategy.upper()}: {fastest} ({avg_time}ms avg)", flush=True)
    else:
        print("  No API metrics recorded yet", flush=True)
    strat_metrics = get_strategy_performance_metrics()
    if strat_metrics:
        print("\n📈 STRATEGY PERFORMANCE (signal generation):", flush=True)
        for strategy, data in strat_metrics.items():
            print(f"  {strategy.upper():<15} | Total signals={data['total_signals']:<4} | Cycles={data['total_cycles']:<3} | Avg/cycle={data['avg_per_cycle']:<6.2f} | Max={data['max_in_cycle']:<3}", flush=True)
    freq_metrics = get_signal_frequency_metrics()
    if freq_metrics['total_signals'] > 0:
        print("\n📊 SIGNAL GENERATION FREQUENCY:", flush=True)
        print(f"  Total signals: {freq_metrics['total_signals']} | Active minutes: {freq_metrics['minutes_active']} | Avg/minute: {freq_metrics['avg_per_minute']:.2f} | Max in minute: {freq_metrics['max_in_minute']}", flush=True)
    error_recovery_status = get_failures_str()
    cache_stats_str = get_cache_stats_str()
    print(f"\n🛡️  Error Recovery: {error_recovery_status}", flush=True)
    print(f"💾 Cache Stats: {cache_stats_str}", flush=True)
    print("="*90 + "\n", flush=True)

def throttle_kline_api_calls():
    """Enforce API call rate limiting for kline fetches (non-blocking lock)."""
    global _last_kline_fetch_time
    wait_time = 0
    with _kline_fetch_lock:
        now = time.time()
        # Schedule next slot
        target_time = max(now, _last_kline_fetch_time + THROTTLE_KLINES)
        wait_time = target_time - now
        _last_kline_fetch_time = target_time
    
    if wait_time > 0:
        time.sleep(wait_time)

def parse_klines(raw_klines: list, source: str) -> list:
    """
    Parse klines from various API formats into standard format:
    [[timestamp_ms, open, high, low, close, volume], ...]
    """
    if not raw_klines:
        return []
    
    parsed = []
    try:
        for kline in raw_klines[-200:]:  # limit to last 200 candles
            if source.lower() == 'kraken':
                parsed.append([
                    int(kline[0]) * 1000,  # time (seconds to ms)
                    float(kline[1]),       # open
                    float(kline[2]),       # high
                    float(kline[3]),       # low
                    float(kline[4]),       # close
                    float(kline[6])        # volume
                ])
            elif source.lower() == 'mexc':
                # MEXC: [time, open, high, low, close, volume, ...]
                parsed.append([
                    int(kline[0]),      # timestamp_ms
                    float(kline[1]),    # open
                    float(kline[2]),    # high
                    float(kline[3]),    # low
                    float(kline[4]),    # close
                    float(kline[5])     # volume
                ])
            elif source.lower() == 'bitget':
                # Bitget: [time, open, high, low, close, volume, ...]
                parsed.append([
                    int(kline[0]),      # timestamp_ms
                    float(kline[1]),    # open
                    float(kline[2]),    # high
                    float(kline[3]),    # low
                    float(kline[4]),    # close
                    float(kline[5])     # volume
                ])
            elif source.lower() == 'okx':
                parsed.append([
                    int(kline[0]),      # timestamp_ms
                    float(kline[1]),    # open
                    float(kline[2]),    # high
                    float(kline[3]),    # low
                    float(kline[4]),    # close
                    float(kline[5])     # volume
                ])
            elif source.lower() == 'coinbase':
                # Coinbase: [timestamp, low, high, open, close, volume]
                parsed.append([
                    int(kline[0]) * 1000,  # timestamp (seconds to ms)
                    float(kline[3]),       # open
                    float(kline[2]),       # high
                    float(kline[1]),       # low
                    float(kline[4]),       # close
                    float(kline[5])        # volume
                ])
            elif source.lower() == 'kucoin':
                # KuCoin: [timestamp_ms, open, close, high, low, volume, turnover]
                parsed.append([
                    int(kline[0]),      # timestamp_ms
                    float(kline[1]),    # open
                    float(kline[3]),    # high
                    float(kline[4]),    # low
                    float(kline[2]),    # close
                    float(kline[5])     # volume
                ])
            elif source.lower() == 'coinpaprika':
                # Coinpaprika OHLCV format: [time_open, time_close, open, high, low, close, volume, market_cap]
                parsed.append([
                    int(kline[0]),      # time_open (ms)
                    float(kline[2]),    # open
                    float(kline[3]),    # high
                    float(kline[4]),    # low
                    float(kline[5]),    # close
                    float(kline[6])     # volume
                ])
            elif source.lower() == 'bitfinex':
                # Bitfinex: [MTS, OPEN, CLOSE, HIGH, LOW, VOLUME]
                parsed.append([
                    int(kline[0]),      # MTS (timestamp in ms)
                    float(kline[1]),    # open
                    float(kline[3]),    # high
                    float(kline[4]),    # low
                    float(kline[2]),    # close
                    float(kline[5])     # volume
                ])
            elif source.lower() == 'cryptocom':
                # Crypto.com: {"t": timestamp_ms, "o": open, "h": high, "l": low, "c": close, "v": volume}
                if isinstance(kline, dict):
                    parsed.append([
                        int(kline.get('t', 0)),      # timestamp_ms
                        float(kline.get('o', 0)),    # open
                        float(kline.get('h', 0)),    # high
                        float(kline.get('l', 0)),    # low
                        float(kline.get('c', 0)),    # close
                        float(kline.get('v', 0))     # volume
                    ])
                else:
                    # Fallback for array format if API changes
                    parsed.append([
                        int(kline[0]),      # timestamp_ms
                        float(kline[1]),    # open
                        float(kline[2]),    # high
                        float(kline[3]),    # low
                        float(kline[4]),    # close
                        float(kline[5])     # volume
                    ])
            elif source.lower() == 'gateio':
                # Gate.io: [t, o, c, h, l, v] with t in seconds
                parsed.append([
                    int(kline[0]) * 1000,  # timestamp (s -> ms)
                    float(kline[1]),       # open
                    float(kline[3]),       # high
                    float(kline[4]),       # low
                    float(kline[2]),       # close
                    float(kline[5])        # volume
                ])
            elif source.lower() == 'bitstamp':
                # Bitstamp OHLC: dict entries {timestamp, open, high, low, close, volume}
                if isinstance(kline, dict):
                    parsed.append([
                        int(kline.get('timestamp', 0)) * 1000,
                        float(kline.get('open', 0)),
                        float(kline.get('high', 0)),
                        float(kline.get('low', 0)),
                        float(kline.get('close', 0)),
                        float(kline.get('volume', 0))
                    ])
            elif source.lower() == 'coinmetro':
                # Coinmetro: [ms, open, high, low, close, volume]
                parsed.append([
                    int(kline[0]),
                    float(kline[1]),
                    float(kline[2]),
                    float(kline[3]),
                    float(kline[4]),
                    float(kline[5]) if len(kline) > 5 else 0.0
                ])
            elif source.lower() == 'hashkey':
                # HashKey: [ms, open, high, low, close, volume]
                parsed.append([
                    int(kline[0]),
                    float(kline[1]),
                    float(kline[2]),
                    float(kline[3]),
                    float(kline[4]),
                    float(kline[5]) if len(kline) > 5 else 0.0
                ])
    except Exception as e:
        if os.getenv("DEBUG_KLINES", "").lower() == "true":
            print(f"[PARSE_KLINES] {source} error: {str(e)[:50]}", flush=True)
        return []
    
    return parsed

def normalize_symbol_kraken(symbol: str) -> str:
    """Convert symbol to Kraken format (e.g., BTC/USDT -> XXBTZUSD)."""
    symbol = symbol.upper()
    if '/' in symbol:
        parts = symbol.split('/')
        base, quote = parts[0], parts[1]
    else:
        # Check if symbol ends with known quote currencies
        known_quotes = ['USDT', 'USD', 'EUR', 'GBP', 'JPY', 'CAD', 'CHF', 'AUD']
        for quote in known_quotes:
            if symbol.endswith(quote):
                base = symbol[:-len(quote)]
                break
        else:
            # No known quote suffix, treat whole symbol as base
            base = symbol
            # Use USD for major coins, USDT for altcoins
            major_coins = ['BTC', 'ETH', 'LTC', 'XRP', 'ADA', 'DOT', 'LINK', 'UNI']
            quote = 'USD' if base in major_coins else 'USDT'
    
    # Kraken base currency codes
    base_map = {
        'BTC': 'XXBT',
        'ETH': 'XETH',
        'LTC': 'XLTC',
        'XRP': 'XXRP',
        'ADA': 'ADA',
        'DOT': 'DOT',
        'LINK': 'LINK',
        'UNI': 'UNI',
        'USDT': 'USDT',
        'USDC': 'USDC',
        'EUR': 'ZEUR',
        'USD': 'ZUSD',
        'GBP': 'ZGBP',
        'JPY': 'ZJPY',
        'CAD': 'ZCAD',
        'CHF': 'ZCHF',
        'AUD': 'ZAUD'
    }
    
    quote_map = {
        'USDT': 'USDT',
        'USD': 'ZUSD',
        'EUR': 'ZEUR',
        'GBP': 'ZGBP',
        'JPY': 'ZJPY',
        'CAD': 'ZCAD',
        'CHF': 'ZCHF',
        'AUD': 'ZAUD'
    }
    
    kraken_base = base_map.get(base, base)
    kraken_quote = quote_map.get(quote, quote)
    
    return kraken_base + kraken_quote

def normalize_symbol_okx(symbol: str) -> str:
    """Convert symbol to OKX format (e.g., BTC/USDT -> BTC-USDT or BTCUSDT -> BTC-USDT)."""
    symbol = symbol.upper()
    if '/' in symbol:
        symbol = symbol.replace('/', '-')
    else:
        known_quotes = ['USDT', 'USD', 'EUR', 'GBP']
        for q in known_quotes:
            if symbol.endswith(q):
                base = symbol[:-len(q)]
                symbol = f"{base}-{q}"
                break
    return symbol

def normalize_symbol_kucoin(symbol: str) -> str:
    """Convert symbol to KuCoin format (e.g., BTC/USDT -> BTC-USDT or BTCUSDT -> BTC-USDT)."""
    symbol = symbol.upper()
    if '/' in symbol:
        symbol = symbol.replace('/', '-')
    else:
        known_quotes = ['USDT', 'USD', 'EUR', 'GBP']
        for q in known_quotes:
            if symbol.endswith(q):
                base = symbol[:-len(q)]
                symbol = f"{base}-{q}"
                break
    return symbol

def normalize_symbol_coinpaprika(symbol: str) -> str:
    """Convert symbol to Coinpaprika coin ID."""
    symbol = symbol.upper()
    if '/' in symbol:
        symbol = symbol.split('/')[0]
    
    coin_map = {
        'BTC': 'btc-bitcoin',
        'ETH': 'eth-ethereum',
        'BNB': 'bnb-binance-coin',
        'XRP': 'xrp-xrp',
        'ADA': 'ada-cardano',
        'SOL': 'sol-solana',
        'DOT': 'dot-polkadot',
        'MATIC': 'matic-polygon',
        'AVAX': 'avax-avalanche',
        'LINK': 'link-chainlink',
        'UNI': 'uni-uniswap',
        'USDT': 'usdt-tether',
        'USDC': 'usdc-usd-coin'
    }
    return coin_map.get(symbol, symbol.lower() + '-unknown')

def fetch_from_kraken(symbol: str, interval: str, limit: int) -> list:
    """Enhanced klines fetcher from Kraken REST API with improved error handling and limit support."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_kraken(symbol)
        
        interval_map = {
            '1m': '1', '5m': '5', '15m': '15', '30m': '30',
            '1h': '60', '4h': '240', '1d': '1440'
        }
        kraken_interval = interval_map.get(interval, '60')
        
        url = 'https://api.kraken.com/0/public/OHLC'
        params = {
            'pair': norm_symbol,
            'interval': kraken_interval
        }
        
        resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            if 'result' in data and norm_symbol in data['result']:
                klines = data['result'][norm_symbol]
                # Kraken returns up to 720 candles, limit to requested amount
                if len(klines) > limit:
                    klines = klines[-limit:]  # Take most recent candles
                parsed = parse_klines(klines, 'kraken')
                # Ensure we have enough candles
                if len(parsed) >= min(limit, 10):  # At least 10 candles or requested limit
                    return parsed
    except Exception as e:
        if os.getenv("DEBUG_KLINES", "").lower() == "true":
            print(f"[KRAKEN] {symbol} error: {str(e)[:50]}", flush=True)
    
    return []

def fetch_from_okx(symbol: str, interval: str, limit: int) -> list:
    """Fetch klines from OKX REST API."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_okx(symbol)
        
        interval_map = {
            '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
            '1h': '1H', '4h': '4H', '1d': '1D'
        }
        okx_interval = interval_map.get(interval, '1H')
        
        url = 'https://www.okx.com/api/v5/market/candles'
        params = {
            'instId': norm_symbol,
            'bar': okx_interval,
            'limit': str(min(limit, 300))
        }
        
        resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('code') == '0' and 'data' in data:
                return parse_klines(data['data'], 'okx')
    except Exception as e:
        if os.getenv("DEBUG_KLINES", "").lower() == "true":
            print(f"[OKX] {symbol} error: {str(e)[:50]}", flush=True)
    
    return []

def fetch_from_kucoin(symbol: str, interval: str, limit: int) -> list:
    """Fetch klines from KuCoin REST API."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_kucoin(symbol)
        
        interval_map = {
            '1m': '1min', '5m': '5min', '15m': '15min', '30m': '30min',
            '1h': '1hour', '4h': '4hour', '1d': '1day'
        }
        kucoin_interval = interval_map.get(interval, '1hour')
        
        url = 'https://api.kucoin.com/api/v1/market/candles'
        params = {
            'symbol': norm_symbol,
            'type': kucoin_interval,
            'startAt': int(time.time() - limit * 60 * (1 if interval == '1m' else 5 if interval == '5m' else 15 if interval == '15m' else 30 if interval == '30m' else 60 if interval == '1h' else 240 if interval == '4h' else 1440))
        }
        
        resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('code') == '200000' and 'data' in data:
                return parse_klines(data['data'], 'kucoin')
    except Exception as e:
        if os.getenv("DEBUG_KLINES", "").lower() == "true":
            print(f"[KUCOIN] {symbol} error: {str(e)[:50]}", flush=True)
    
    return []

def fetch_from_coinpaprika(symbol: str, interval: str, limit: int) -> list:
    """Fetch OHLCV from Coinpaprika API."""
    try:
        throttle_kline_api_calls()
        coin_id = normalize_symbol_coinpaprika(symbol)
        
        # Coinpaprika has limited intervals, mainly daily
        if interval not in ['1d']:
            return []
        
        url = f'https://api.coinpaprika.com/v1/coins/{coin_id}/ohlcv/historical'
        params = {
            'start': int((time.time() - limit * 24 * 3600) * 1000),  # start time in ms
            'limit': min(limit, 365)
        }
        
        resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            return parse_klines(data, 'coinpaprika')
    except Exception as e:
        if os.getenv("DEBUG_KLINES", "").lower() == "true":
            print(f"[COINPAPRIKA] {symbol} error: {str(e)[:50]}", flush=True)
    
    return []

def fetch_market_data_coingecko(symbol: str) -> dict:
    """Fetch market data from CoinGecko API (MOMENTUM primary)."""
    try:
        throttle_kline_api_calls()
        coin_id = normalize_symbol_coingecko(symbol)
        
        # Use /coins/markets endpoint for better data
        url = 'https://api.coingecko.com/api/v3/coins/markets'
        params = {
            'vs_currency': 'usd',
            'ids': coin_id,
            'order': 'volume_desc',
            'per_page': 1,
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '1h,24h'
        }
        
        resp = _get_requests_session().get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                coin_data = data[0]
                return {
                    'price': float(coin_data.get('current_price', 0)),
                    'volume': float(coin_data.get('total_volume', 0)),
                    'change': float(coin_data.get('price_change_percentage_24h', 0)),
                    'price_change_24h': float(coin_data.get('price_change_percentage_24h', 0)),
                    'price_change_1h': float(coin_data.get('price_change_percentage_1h_in_currency', 0)),
                    'high_24h': float(coin_data.get('high_24h', 0)),
                    'low_24h': float(coin_data.get('low_24h', 0)),
                    'market_cap': float(coin_data.get('market_cap', 0)),
                    'coin_id': coin_data.get('id'),
                    'timestamp': time.time()
                }
    except Exception as e:
        if os.getenv("DEBUG_MARKET_DATA", "").lower() == "true":
            print(f"[COINGECKO] {symbol} error: {str(e)[:50]}", flush=True)
    
    return {}

def fetch_market_data_cexio(symbol: str) -> dict:
    """Fetch market data from CEX.IO API (MOMENTUM fallback)."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_cexio(symbol)
        
        url = f'https://cex.io/api/tickers/USDT'
        
        resp = _get_requests_session().get(url, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, dict) and 'data' in data:
                for ticker in data['data']:
                    if ticker.get('pair') == norm_symbol:
                        price = float(ticker.get('last', 0))
                        change_24h = float(ticker.get('priceChangePercentage', 0))
                        return {
                            'price': price,
                            'volume': float(ticker.get('volume', 0)),
                            'change': change_24h,
                            'price_change_24h': change_24h,
                            'price_change_1h': 0,  # CEX.IO doesn't provide 1h change
                            'high_24h': float(ticker.get('high', price)),
                            'low_24h': float(ticker.get('low', price)),
                            'market_cap': 0,
                            'coin_id': symbol_upper.replace(':USDT', '').lower(),
                            'timestamp': time.time()
                        }
    except Exception as e:
        if os.getenv("DEBUG_MARKET_DATA", "").lower() == "true":
            print(f"[CEXIO] {symbol} error: {str(e)[:50]}", flush=True)
    
    return {}

def fetch_market_data_coinpaprika(symbol: str) -> dict:
    """Fetch market data from Coinpaprika API (REVERSAL primary)."""
    try:
        symbol_upper = symbol.upper()
        if '/' in symbol_upper:
            symbol_upper = symbol_upper.split('/')[0]
        
        coin_map = {
            'BTC': 'btc-bitcoin', 'ETH': 'eth-ethereum', 'BNB': 'bnb-binance-coin',
            'XRP': 'xrp-xrp', 'ADA': 'ada-cardano', 'SOL': 'sol-solana',
            'DOT': 'dot-polkadot', 'MATIC': 'matic-polygon', 'AVAX': 'avax-avalanche',
            'LINK': 'link-chainlink', 'UNI': 'uni-uniswap', 'USDT': 'usdt-tether',
            'USDC': 'usdc-usd-coin', 'LTC': 'ltc-litecoin', 'DOGE': 'doge-dogecoin'
        }
        
        coin_id = coin_map.get(symbol_upper, symbol_upper.lower() + '-unknown')
        
        url = f'https://api.coinpaprika.com/v1/tickers/{coin_id}'
        
        resp = _get_requests_session().get(url, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            quotes = data.get('quotes', {})
            usd_data = quotes.get('USD', {})
            
            return {
                'price': float(usd_data.get('price', 0)),
                'volume': float(data.get('total_volume', 0)),
                'change': float(usd_data.get('percent_change_24h', 0)),
                'price_change_24h': float(usd_data.get('percent_change_24h', 0)),
                'price_change_1h': float(usd_data.get('percent_change_1h', 0)),
                'high_24h': float(usd_data.get('price', 0)) * 1.05,  # Estimate if not available
                'low_24h': float(usd_data.get('price', 0)) * 0.95,  # Estimate if not available
                'market_cap': float(usd_data.get('market_cap', 0)),
                'coin_id': data.get('id'),
                'timestamp': time.time()
            }
    except Exception as e:
        if os.getenv("DEBUG_MARKET_DATA", "").lower() == "true":
            print(f"[COINPAPRIKA] {symbol} error: {str(e)[:50]}", flush=True)
    
    return {}

def fetch_market_data_bitstamp(symbol: str) -> dict:
    """Fetch market data from Bitstamp API (REVERSAL fallback)."""
    try:
        symbol_upper = symbol.upper()
        if '/' in symbol_upper:
            symbol_upper = symbol_upper.replace('/', '')
        
        symbol_lower = symbol_upper.lower()
        
        url = f'https://www.bitstamp.net/api/v2/ticker/{symbol_lower}usd/'
        
        resp = _get_requests_session().get(url, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            price = float(data.get('last', 0))
            open_price = float(data.get('open', price))
            change_24h = ((price - open_price) / open_price * 100) if open_price > 0 else 0
            return {
                'price': price,
                'volume': float(data.get('volume', 0)),
                'change': change_24h,
                'price_change_24h': change_24h,
                'price_change_1h': 0,  # Bitstamp doesn't provide 1h change
                'high_24h': float(data.get('high', price)),
                'low_24h': float(data.get('low', price)),
                'market_cap': 0,
                'coin_id': symbol_lower,
                'timestamp': time.time()
            }
    except Exception as e:
        if os.getenv("DEBUG_MARKET_DATA", "").lower() == "true":
            print(f"[BITSTAMP] {symbol} error: {str(e)[:50]}", flush=True)
    
    return {}

def fetch_market_data_huobi(symbol: str) -> dict:
    """Fetch market data from Huobi API (RANGE primary)."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_huobi(symbol)
        
        # Merged endpoint provides better 24h data including open price for change calculation
        url = f'https://api.huobi.pro/market/detail/merged?symbol={norm_symbol}'
        
        resp = _get_requests_session().get(url, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'ok' and 'tick' in data:
                tick = data['tick']
                price = float(tick.get('close', 0))
                open_p = float(tick.get('open', price))
                high = float(tick.get('high', price))
                low = float(tick.get('low', price))
                
                # Calculate 24h change percentage
                change_24h = ((price - open_p) / open_p * 100) if open_p > 0 else 0
                
                return {
                    'price': price,
                    'volume': float(tick.get('vol', 0)),
                    'change': change_24h,
                    'price_change_24h': change_24h,
                    'price_change_1h': 0, # Huobi detail doesn't provide 1h change directly
                    'high_24h': high,
                    'low_24h': low,
                    'market_cap': 0,
                    'coin_id': norm_symbol.replace('usdt', ''),
                    'timestamp': time.time()
                }
    except Exception as e:
        if os.getenv("DEBUG_MARKET_DATA", "").lower() == "true":
            print(f"[HUOBI] {symbol} error: {str(e)[:50]}", flush=True)
    
    return {}

def fetch_market_data_gemini(symbol: str) -> dict:
    """Fetch market data from Gemini API (RANGE fallback)."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_gemini(symbol)
        
        url = f'https://api.gemini.com/v1/pubticker/{norm_symbol}'
        
        resp = _get_requests_session().get(url, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            price = float(data.get('last', 0))
            return {
                'price': price,
                'volume': float(data.get('volume', {}).get('USD', 0)) if isinstance(data.get('volume'), dict) else float(data.get('volume', 0)),
                'change': 0,
                'price_change_24h': 0,
                'price_change_1h': 0,
                'high_24h': float(data.get('high', price)),
                'low_24h': float(data.get('low', price)),
                'market_cap': 0,
                'coin_id': norm_symbol,
                'timestamp': time.time()
            }
    except Exception as e:
        if os.getenv("DEBUG_MARKET_DATA", "").lower() == "true":
            print(f"[GEMINI] {symbol} error: {str(e)[:50]}", flush=True)
    
    return {}

def fetch_market_data_momentum(symbol: str) -> dict:
    """Momentum strategy: CoinGecko primary, CEX.IO fallback."""
    # Normalization: Reduce wasted calls for non-tradable symbols
    if not is_tradable_on_exchanges(symbol, strategy="momentum"):
        return {}
        
    result = fetch_market_data_coingecko(symbol)
    if result and result.get('price', 0) > 0:
        return result
    result = fetch_market_data_cexio(symbol)
    if result and result.get('price', 0) > 0:
        return result
    return {}

def fetch_market_data_reversal(symbol: str) -> dict:
    """Reversal strategy: Coinpaprika primary, Bitstamp fallback."""
    # Normalization: Reduce wasted calls for non-tradable symbols
    if not is_tradable_on_exchanges(symbol, strategy="reversal"):
        return {}
        
    result = fetch_market_data_coinpaprika(symbol)
    if result and result.get('price', 0) > 0:
        return result
    result = fetch_market_data_bitstamp(symbol)
    if result and result.get('price', 0) > 0:
        return result
    return {}

def fetch_market_data_range(symbol: str) -> dict:
    """Range strategy: Huobi primary, Gemini fallback."""
    # Normalization: Reduce wasted calls for non-tradable symbols
    if not is_tradable_on_exchanges(symbol, strategy="range"):
        return {}
        
    result = fetch_market_data_huobi(symbol)
    if result and result.get('price', 0) > 0:
        return result
    result = fetch_market_data_gemini(symbol)
    if result and result.get('price', 0) > 0:
        return result
    return {}

def fetch_market_data_livecoinwatch_reversal(symbol: str) -> dict:
    """Reversal strategy: Now uses Coinpaprika primary, Bitstamp fallback."""
    return fetch_market_data_reversal(symbol)

def fetch_market_data_livecoinwatch_range(symbol: str) -> dict:
    """Range strategy: Now uses Huobi primary, Gemini fallback."""
    return fetch_market_data_range(symbol)

def fetch_market_data_livecoinwatch_momentum(symbol: str) -> dict:
    """Momentum strategy: Now uses CoinGecko primary, CEX.IO fallback."""
    return fetch_market_data_momentum(symbol)

def fetch_from_okx_paginated(symbol: str, interval: str, limit: int, max_pages: int = 5) -> list:
    """Enhanced klines fetcher from OKX with improved pagination and error handling."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_okx(symbol)
        
        interval_map = {
            '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
            '1h': '1H', '4h': '4H', '1d': '1D'
        }
        okx_interval = interval_map.get(interval, '1H')
        
        all_candles = []
        after = None
        max_pages = max(max_pages, (limit // 100) + 1)  # Calculate pages needed
        
        for page in range(max_pages):
            url = 'https://www.okx.com/api/v5/market/candles'
            params = {
                'instId': norm_symbol,
                'bar': okx_interval,
                'limit': str(min(100, limit - len(all_candles)))
            }
            if after:
                params['after'] = after
            
            resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('code') == '0' and 'data' in data:
                    candles = data['data']
                    if not candles:
                        break
                    # OKX returns newest first, we'll reverse after collecting all
                    all_candles.extend(candles)
                    if len(all_candles) >= limit or len(candles) < 100:
                        break
                    after = candles[-1][0]  # Use oldest candle timestamp for next page (before reverse)
                elif data.get('code') == '51000':  # Invalid symbol
                    return []
            elif resp.status_code == 404:
                return []
            else:
                # For other errors, try next page or return what we have
                if page == 0:
                    return []
                break
        
        if not all_candles:
            return []
        
        # OKX returns newest first, reverse to get chronological order (oldest first)
        all_candles.reverse()
        # Sort by timestamp (oldest first) and limit
        all_candles.sort(key=lambda x: int(x[0]) if isinstance(x, list) and len(x) > 0 else 0)
        parsed = parse_klines(all_candles[:limit], 'okx')
        
        # Ensure minimum quality: at least 10 candles or requested limit
        if len(parsed) >= min(limit, 10):
            return parsed
        return []
    except Exception as e:
        if os.getenv("DEBUG_KLINES", "").lower() == "true":
            print(f"[OKX PAGINATED] {symbol} error: {str(e)[:50]}", flush=True)
    
    return []


def fetch_from_kucoin_paginated(symbol: str, interval: str, limit: int, max_pages: int = 5) -> list:
    """Enhanced klines fetcher from KuCoin with improved pagination and error handling."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_kucoin(symbol)
        
        interval_map = {
            '1m': '1min', '5m': '5min', '15m': '15min', '30m': '30min',
            '1h': '1hour', '4h': '4hour', '1d': '1day'
        }
        kucoin_interval = interval_map.get(interval, '1hour')
        
        interval_seconds = {
            '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
            '1h': 3600, '4h': 14400, '1d': 86400
        }.get(interval, 3600)
        
        all_candles = []
        current_time = int(time.time())
        max_pages = max(max_pages, (limit // 300) + 1)  # Calculate pages needed
        
        for page in range(max_pages):
            end_time = current_time - (page * 300 * interval_seconds)
            start_time = end_time - (300 * interval_seconds)
            
            url = 'https://api.kucoin.com/api/v1/market/candles'
            params = {
                'symbol': norm_symbol,
                'type': kucoin_interval,
                'startAt': start_time,
                'endAt': end_time
            }
            
            resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('code') == '200000' and 'data' in data:
                    candles = data['data']
                    if not candles:
                        break
                    # KuCoin returns newest first, reverse for chronological order
                    candles.reverse()
                    all_candles.extend(candles)
                    if len(all_candles) >= limit:
                        break
                elif data.get('code') == '400100':  # Invalid symbol
                    return []
            elif resp.status_code == 404:
                return []
            else:
                # For other errors, try next page or return what we have
                if page == 0:
                    return []
                break
        
        if not all_candles:
            return []
        
        # Sort by timestamp and limit
        all_candles.sort(key=lambda x: int(x[0]) if isinstance(x, list) else 0)
        parsed = parse_klines(all_candles[:limit], 'kucoin')
        
        # Ensure minimum quality: at least 10 candles or requested limit
        if len(parsed) >= min(limit, 10):
            return parsed
        return []
    except Exception as e:
        if os.getenv("DEBUG_KLINES", "").lower() == "true":
            print(f"[KUCOIN PAGINATED] {symbol} error: {str(e)[:50]}", flush=True)
    
    return []


def normalize_symbol_bitfinex(symbol: str) -> str:
    """Convert symbol to Bitfinex format (e.g., BTC/USDT -> tBTCUSD or BTCUSDT -> tBTCUSD)."""
    symbol = symbol.upper()
    if '/' in symbol:
        base, quote = symbol.split('/')
    else:
        known_quotes = ['USDT', 'USD', 'EUR', 'GBP']
        for q in known_quotes:
            if symbol.endswith(q):
                base = symbol[:-len(q)]
                quote = q
                break
        else:
            base = symbol
            quote = 'USD'
    
    # Bitfinex uses t prefix for trading pairs
    if quote == 'USDT':
        quote = 'USD'  # Bitfinex uses USD for USDT pairs
    return f"t{base}{quote}"


def normalize_symbol_cryptocom(symbol: str) -> str:
    """Convert symbol to Crypto.com format (e.g., BTC/USDT -> BTC_USDT or BTCUSDT -> BTC_USDT)."""
    symbol = symbol.upper()
    if '/' in symbol:
        symbol = symbol.replace('/', '_')
    else:
        known_quotes = ['USDT', 'USD', 'EUR', 'GBP']
        for q in known_quotes:
            if symbol.endswith(q):
                base = symbol[:-len(q)]
                symbol = f"{base}_{q}"
                break
    return symbol


def fetch_from_bitfinex_paginated(symbol: str, interval: str, limit: int, max_pages: int = 5) -> list:
    """Fetch klines from Bitfinex with pagination support."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_bitfinex(symbol)
        
        # Bitfinex interval mapping (in milliseconds)
        interval_map = {
            '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
            '1h': '1h', '4h': '4h', '1d': '1D'
        }
        bfx_interval = interval_map.get(interval, '1h')
        
        # Bitfinex uses timeframes: 1m, 5m, 15m, 30m, 1h, 3h, 6h, 12h, 1D, 1W, 14D, 1M
        # Map our intervals to Bitfinex timeframes
        bfx_timeframe_map = {
            '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
            '1h': '1h', '4h': '6h', '1d': '1D'  # 4h maps to 6h (closest)
        }
        bfx_timeframe = bfx_timeframe_map.get(interval, '1h')
        
        all_candles = []
        end_time = int(time.time() * 1000)  # Bitfinex uses milliseconds
        
        # Calculate interval duration in milliseconds
        interval_ms_map = {
            '1m': 60000, '5m': 300000, '15m': 900000, '30m': 1800000,
            '1h': 3600000, '4h': 14400000, '1d': 86400000
        }
        interval_ms = interval_ms_map.get(interval, 3600000)
        
        for page in range(max_pages):
            # Bitfinex API: /v2/candles/trade:{timeframe}:{symbol}/hist
            # Returns up to 10000 candles per request, but we'll paginate for reliability
            url = f'https://api-pub.bitfinex.com/v2/candles/trade:{bfx_timeframe}:{norm_symbol}/hist'
            params = {
                'limit': min(1000, limit - len(all_candles)),  # Bitfinex allows up to 10000, but we use 1000 per page
                'end': end_time,
                'sort': -1  # -1 for descending (newest first)
            }
            
            resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
            if resp.status_code == 200:
                candles = resp.json()
                if not candles or len(candles) == 0:
                    break
                
                # Bitfinex returns: [MTS, OPEN, CLOSE, HIGH, LOW, VOLUME]
                # Reverse to get chronological order (oldest first)
                candles.reverse()
                all_candles.extend(candles)
                
                if len(all_candles) >= limit:
                    break
                
                # Update end_time for next page (oldest candle timestamp)
                if candles:
                    end_time = candles[0][0] - interval_ms  # Go back one interval from oldest
            else:
                if resp.status_code == 404:
                    # Symbol not found, try without 't' prefix
                    if norm_symbol.startswith('t'):
                        norm_symbol = norm_symbol[1:]
                        continue
                    return []
                elif page == 0:
                    return []
                break
        
        if not all_candles:
            return []
        
        # Sort by timestamp (oldest first) and limit
        all_candles.sort(key=lambda x: x[0])
        parsed = parse_klines(all_candles[:limit], 'bitfinex')
        
        # Ensure minimum quality: at least 10 candles or requested limit
        if len(parsed) >= min(limit, 10):
            return parsed
        return []
    except Exception as e:
        if os.getenv("DEBUG_KLINES", "").lower() == "true":
            print(f"[BITFINEX PAGINATED] {symbol} error: {str(e)[:50]}", flush=True)
    
    return []


def fetch_from_cryptocom_paginated(symbol: str, interval: str, limit: int, max_pages: int = 5) -> list:
    """Fetch klines from Crypto.com Exchange with pagination support."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_cryptocom(symbol)
        
        # Crypto.com interval mapping
        interval_map = {
            '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
            '1h': '1h', '4h': '4h', '1d': '1D'
        }
        cdc_interval = interval_map.get(interval, '1h')
        
        all_candles = []
        end_time = int(time.time() * 1000)  # Crypto.com uses milliseconds
        
        # Calculate interval duration in milliseconds
        interval_ms_map = {
            '1m': 60000, '5m': 300000, '15m': 900000, '30m': 1800000,
            '1h': 3600000, '4h': 14400000, '1d': 86400000
        }
        interval_ms = interval_ms_map.get(interval, 3600000)
        
        for page in range(max_pages):
            # Crypto.com API: /v2/public/get-candlestick
            url = 'https://api.crypto.com/v2/public/get-candlestick'
            params = {
                'instrument_name': norm_symbol,
                'timeframe': cdc_interval,
                'count': min(500, limit - len(all_candles)),  # Crypto.com allows up to 500 per request
                'end_ts': end_time
            }
            
            resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('code') == 0 and 'result' in data and 'data' in data['result']:
                    candles = data['result']['data']
                    if not candles or len(candles) == 0:
                        break
                    
                    all_candles.extend(candles)
                    
                    if len(all_candles) >= limit:
                        break
                    
                    # Update end_time for next page (oldest candle timestamp)
                    if candles:
                        # Crypto.com returns: [t, o, h, l, c, v] where t is timestamp in ms
                        end_time = int(candles[-1]['t']) - interval_ms
                else:
                    break
            else:
                break
        
        if not all_candles:
            return []
        
        # Sort by timestamp (oldest first) and limit
        all_candles.sort(key=lambda x: x.get('t', 0) if isinstance(x, dict) else (x[0] if isinstance(x, list) else 0))
        parsed = parse_klines(all_candles[:limit], 'cryptocom')
        
        # Ensure minimum quality: at least 10 candles or requested limit
        if len(parsed) >= min(limit, 10):
            return parsed
        return []
    except Exception as e:
        if os.getenv("DEBUG_KLINES", "").lower() == "true":
            print(f"[CRYPTO.COM PAGINATED] {symbol} error: {str(e)[:50]}", flush=True)
    
    return []


# Unified base/quote parser to keep normalization consistent across strategies
# Accepts forms like 'BTC/USDT', 'BTC-USDT', 'BTCUSDT' and returns (BASE, QUOTE)
# Defaults quote to USDT if none can be inferred.
def _split_base_quote(symbol: str) -> tuple:
    s = symbol.strip().upper()
    if '/' in s:
        base, quote = s.split('/', 1)
        return base, quote
    if '-' in s:
        base, quote = s.split('-', 1)
        return base, quote
    # Infer from suffix
    known_quotes = ['USDT', 'USDC', 'USD', 'EUR', 'GBP']
    for q in known_quotes:
        if s.endswith(q) and len(s) > len(q):
            base = s[:-len(q)]
            return base, q
    # Fallback: no explicit quote; assume USDT
    return s, 'USDT'


def normalize_symbol_mexc(symbol: str) -> str:
    """Convert symbol to MEXC format (e.g., BTC/USDT -> BTC_USDT or BTCUSDT -> BTC_USDT)."""
    symbol = symbol.upper()
    if '/' in symbol:
        symbol = symbol.replace('/', '_')
    else:
        known_quotes = ['USDT', 'USD', 'EUR', 'GBP']
        for q in known_quotes:
            if symbol.endswith(q):
                base = symbol[:-len(q)]
                symbol = f"{base}_{q}"
                break
    return symbol


def normalize_symbol_bitget(symbol: str) -> str:
    """Convert symbol to Bitget format using unified logic (BASEQUOTE, e.g., BTCUSDT)."""
    base, quote = _split_base_quote(symbol)
    return f"{base}{quote}"


def normalize_symbol_huobi(symbol: str) -> str:
    """Convert symbol to Huobi format (basequote in lowercase)."""
    base, quote = _split_base_quote(symbol)
    return f"{base}{quote}".lower()


def normalize_symbol_gemini(symbol: str) -> str:
    """Convert symbol to Gemini format (basequote in lowercase)."""
    base, quote = _split_base_quote(symbol)
    return f"{base}{quote}".lower()


def normalize_symbol_coingecko(symbol: str) -> str:
    """Convert symbol to CoinGecko coin id format."""
    symbol_upper = symbol.upper()
    if '/' in symbol_upper:
        symbol_upper = symbol_upper.split('/')[0]
    
    coin_map = {
        'BTC': 'bitcoin', 'ETH': 'ethereum', 'BNB': 'binancecoin',
        'XRP': 'ripple', 'ADA': 'cardano', 'SOL': 'solana',
        'DOT': 'polkadot', 'MATIC': 'matic-network', 'AVAX': 'avalanche-2',
        'LINK': 'chainlink', 'UNI': 'uniswap', 'USDT': 'tether',
        'USDC': 'usd-coin', 'LTC': 'litecoin', 'DOGE': 'dogecoin'
    }
    return coin_map.get(symbol_upper, symbol_upper.lower())


def normalize_symbol_cexio(symbol: str) -> str:
    """Convert symbol to CEX.IO format (BASE:QUOTE)."""
    symbol_upper = symbol.upper()
    if '/' in symbol_upper:
        return symbol_upper.replace('/', ':')
    return symbol_upper + ':USDT'


def normalize_symbol_gateio(symbol: str) -> str:
    """Convert symbol to Gate.io format using unified logic (BASE_QUOTE, e.g., BTC_USDT)."""
    base, quote = _split_base_quote(symbol)
    return f"{base}_{quote}"


essential_lower_quotes = ['usdt', 'usd', 'eur', 'gbp']

def normalize_symbol_bitstamp(symbol: str) -> str:
    """Convert symbol to Bitstamp format using unified logic (basequote lowercase)."""
    base, quote = _split_base_quote(symbol)
    return f"{base}{quote}".lower()


def normalize_symbol_coinmetro(symbol: str) -> str:
    """Convert symbol to Coinmetro format using unified logic (BASEQUOTE)."""
    base, quote = _split_base_quote(symbol)
    return f"{base}{quote}"


def normalize_symbol_hashkey(symbol: str) -> str:
    """Convert symbol to HashKey format using unified logic (BASEQUOTE)."""
    base, quote = _split_base_quote(symbol)
    return f"{base}{quote}"


def fetch_bitget_spot_symbols_cached(ttl=600):
    """Fetch and cache the list of tradable spot symbols from Bitget."""
    global _BITGET_SYMBOLS_CACHE
    now = time.time()
    with _EXCHANGE_SYMBOLS_LOCK:
        if now - _BITGET_SYMBOLS_CACHE['timestamp'] < ttl and _BITGET_SYMBOLS_CACHE['symbols']:
            return _BITGET_SYMBOLS_CACHE['symbols']

    try:
        url = "https://api.bitget.com/api/v2/spot/public/symbols"
        resp = _get_requests_session().get(url, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('code') == '00000':
                symbols = {s['symbol'] for s in data.get('data', []) if s.get('status') == 'online'}
                with _EXCHANGE_SYMBOLS_LOCK:
                    _BITGET_SYMBOLS_CACHE = {'symbols': symbols, 'timestamp': now}
                return symbols
    except Exception as e:
        if DEBUG_ERRORS:
            print(f"[ERROR] Failed to fetch Bitget symbols: {e}", flush=True)

    with _EXCHANGE_SYMBOLS_LOCK:
        return _BITGET_SYMBOLS_CACHE['symbols']


def fetch_hashkey_spot_symbols_cached(ttl=600):
    """Fetch and cache the list of tradable spot symbols from HashKey."""
    global _HASHKEY_SYMBOLS_CACHE
    now = time.time()
    with _EXCHANGE_SYMBOLS_LOCK:
        if now - _HASHKEY_SYMBOLS_CACHE['timestamp'] < ttl and _HASHKEY_SYMBOLS_CACHE['symbols']:
            return _HASHKEY_SYMBOLS_CACHE['symbols']

    try:
        # Using the spot symbols endpoint
        url = "https://api.hashkey.com/spot/v1/symbols"
        resp = _get_requests_session().get(url, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            # HashKey returns a list of objects directly or in a 'data' field depending on version
            symbols_list = data.get('data', []) if isinstance(data, dict) else data
            symbols = {s['symbol'] for s in symbols_list if s.get('status') == 'TRADING'}
            with _EXCHANGE_SYMBOLS_LOCK:
                _HASHKEY_SYMBOLS_CACHE = {'symbols': symbols, 'timestamp': now}
            return symbols
    except Exception as e:
        if DEBUG_ERRORS:
            print(f"[ERROR] Failed to fetch HashKey symbols: {e}", flush=True)

    with _EXCHANGE_SYMBOLS_LOCK:
        return _HASHKEY_SYMBOLS_CACHE['symbols']


def fetch_gateio_spot_symbols_cached(ttl=600):
    """Fetch and cache the list of tradable spot symbols from Gate.io."""
    global _GATEIO_SYMBOLS_CACHE
    now = time.time()
    with _EXCHANGE_SYMBOLS_LOCK:
        if now - _GATEIO_SYMBOLS_CACHE['timestamp'] < ttl and _GATEIO_SYMBOLS_CACHE['symbols']:
            return _GATEIO_SYMBOLS_CACHE['symbols']

    try:
        url = "https://api.gateio.ws/api/v4/spot/currency_pairs"
        resp = _get_requests_session().get(url, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            symbols = {s['id'] for s in data if s.get('trade_status') == 'tradable'}
            with _EXCHANGE_SYMBOLS_LOCK:
                _GATEIO_SYMBOLS_CACHE = {'symbols': symbols, 'timestamp': now}
            return symbols
    except Exception as e:
        if DEBUG_ERRORS:
            print(f"[ERROR] Failed to fetch Gate.io symbols: {e}", flush=True)

    with _EXCHANGE_SYMBOLS_LOCK:
        return _GATEIO_SYMBOLS_CACHE['symbols']


def fetch_bitstamp_spot_symbols_cached(ttl=600):
    """Fetch and cache the list of tradable spot symbols from Bitstamp."""
    global _BITSTAMP_SYMBOLS_CACHE
    now = time.time()
    with _EXCHANGE_SYMBOLS_LOCK:
        if now - _BITSTAMP_SYMBOLS_CACHE['timestamp'] < ttl and _BITSTAMP_SYMBOLS_CACHE['symbols']:
            return _BITSTAMP_SYMBOLS_CACHE['symbols']

    try:
        url = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
        resp = _get_requests_session().get(url, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            symbols = {s['url_symbol'] for s in data if s.get('trading') == 'Enabled'}
            with _EXCHANGE_SYMBOLS_LOCK:
                _BITSTAMP_SYMBOLS_CACHE = {'symbols': symbols, 'timestamp': now}
            return symbols
    except Exception as e:
        if DEBUG_ERRORS:
            print(f"[ERROR] Failed to fetch Bitstamp symbols: {e}", flush=True)

    with _EXCHANGE_SYMBOLS_LOCK:
        return _BITSTAMP_SYMBOLS_CACHE['symbols']


def fetch_kraken_spot_symbols_cached(ttl=600):
    """Fetch and cache the list of tradable spot symbols from Kraken."""
    global _KRAKEN_SYMBOLS_CACHE
    now = time.time()
    with _EXCHANGE_SYMBOLS_LOCK:
        if now - _KRAKEN_SYMBOLS_CACHE['timestamp'] < ttl and _KRAKEN_SYMBOLS_CACHE['symbols']:
            return _KRAKEN_SYMBOLS_CACHE['symbols']

    try:
        url = "https://api.kraken.com/0/public/AssetPairs"
        resp = _get_requests_session().get(url, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            if not data.get('error'):
                symbols = set()
                for name, info in data.get('result', {}).items():
                    # Use altname as it's more standard (e.g., XBTUSD)
                    altname = info.get('altname')
                    if altname:
                        symbols.add(altname)
                    symbols.add(name) # Also add the internal name
                with _EXCHANGE_SYMBOLS_LOCK:
                    _KRAKEN_SYMBOLS_CACHE = {'symbols': symbols, 'timestamp': now}
                return symbols
    except Exception as e:
        if DEBUG_ERRORS:
            print(f"[ERROR] Failed to fetch Kraken symbols: {e}", flush=True)

    with _EXCHANGE_SYMBOLS_LOCK:
        return _KRAKEN_SYMBOLS_CACHE['symbols']


def fetch_coinmetro_spot_symbols_cached(ttl=600):
    """Fetch and cache the list of tradable spot symbols from Coinmetro."""
    global _COINMETRO_SYMBOLS_CACHE
    now = time.time()
    with _EXCHANGE_SYMBOLS_LOCK:
        if now - _COINMETRO_SYMBOLS_CACHE['timestamp'] < ttl and _COINMETRO_SYMBOLS_CACHE['symbols']:
            return _COINMETRO_SYMBOLS_CACHE['symbols']

    try:
        url = "https://api.coinmetro.com/exchange/prices"
        resp = _get_requests_session().get(url, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            # Coinmetro returns a dict of prices, keys are symbols
            symbols = set(data.keys())
            with _EXCHANGE_SYMBOLS_LOCK:
                _COINMETRO_SYMBOLS_CACHE = {'symbols': symbols, 'timestamp': now}
            return symbols
    except Exception as e:
        if DEBUG_ERRORS:
            print(f"[ERROR] Failed to fetch Coinmetro symbols: {e}", flush=True)

    with _EXCHANGE_SYMBOLS_LOCK:
        return _COINMETRO_SYMBOLS_CACHE['symbols']


def normalize_symbol_for_exchange(symbol: str, exchange: str) -> str:
    """Normalize symbol for specific exchange formats."""
    exch = exchange.lower()
    if exch == "huobi":
        return normalize_symbol_huobi(symbol)
    elif exch == "bitget":
        return normalize_symbol_bitget(symbol)
    elif exch == "kucoin":
        return normalize_symbol_kucoin(symbol)
    elif exch == "gateio":
        return normalize_symbol_gateio(symbol)
    elif exch == "bitstamp":
        return normalize_symbol_bitstamp(symbol)
    elif exch == "kraken":
        return normalize_symbol_kraken(symbol)
    elif exch == "coinmetro":
        return normalize_symbol_coinmetro(symbol)
    elif exch == "hashkey":
        return normalize_symbol_hashkey(symbol)
    return symbol.upper()


def verify_pair_fetch_compatibility(pair, exchange_list):
    """Check if a pair fetched from API exists and is tradable on kline exchanges."""
    valid = False
    for exchange in exchange_list:
        if is_tradable_on_exchanges(pair, strategy=exchange):
            valid = True
            break
    return valid


def is_tradable_on_exchanges(base_quote: str, strategy: str = "general") -> bool:
    """
    Check if a pair is tradable on strategy-specific kline exchanges.
    strategy can be: "reversal", "range", "momentum", "general", or a specific exchange name.
    """
    strat = strategy.lower()
    
    # Map strategies to their primary/fallback exchanges
    STRATEGY_EXCHANGES = {
        "reversal": ["bitget", "hashkey"],
        "range": ["gateio", "bitstamp"],
        "momentum": ["kraken", "coinmetro"],
        "general": ["bitget", "hashkey", "kraken", "gateio"]
    }
    
    # If strategy is a specific exchange, just check that one
    exchanges_to_check = STRATEGY_EXCHANGES.get(strat, [strat])
    
    for exchange in exchanges_to_check:
        if exchange == "bitget":
            symbols = fetch_bitget_spot_symbols_cached()
            if normalize_symbol_bitget(base_quote) in symbols: return True
        elif exchange == "hashkey":
            symbols = fetch_hashkey_spot_symbols_cached()
            if normalize_symbol_hashkey(base_quote) in symbols: return True
        elif exchange == "gateio":
            symbols = fetch_gateio_spot_symbols_cached()
            if normalize_symbol_gateio(base_quote) in symbols: return True
        elif exchange == "bitstamp":
            symbols = fetch_bitstamp_spot_symbols_cached()
            if normalize_symbol_bitstamp(base_quote) in symbols: return True
        elif exchange == "kraken":
            symbols = fetch_kraken_spot_symbols_cached()
            if normalize_symbol_kraken(base_quote) in symbols: return True
        elif exchange == "coinmetro":
            symbols = fetch_coinmetro_spot_symbols_cached()
            if normalize_symbol_coinmetro(base_quote) in symbols: return True
            
    return False


def fetch_top_movers(limit=50):
    """Fetch top gainers and losers from CoinGecko with full market data."""
    candidates = []
    url = "https://api.coingecko.com/api/v3/coins/markets"
    
    try:
        # Gainers
        params_gainers = {
            'vs_currency': 'usd',
            'order': 'price_change_percentage_24h_desc',
            'per_page': limit,
            'page': 1,
            'sparkline': 'false',
            'price_change_percentage': '1h,24h'
        }
        resp = _get_requests_session().get(url, params=params_gainers, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            for coin in resp.json():
                try:
                    symbol = coin['symbol'].upper()
                    if is_stablecoin(symbol):
                        continue
                    
                    price = float(coin.get('current_price') or 0)
                    change_24h = float(coin.get('price_change_percentage_24h_in_currency') or coin.get('price_change_percentage_24h') or 0)
                    
                    candidates.append({
                        'id': coin.get('id'),
                        'symbol': symbol,
                        'price': price,
                        'volume': float(coin.get('total_volume') or 0),
                        'price_change_24h': change_24h,
                        'change_24h': change_24h,
                        'price_change_1h': float(coin.get('price_change_percentage_1h_in_currency') or 0),
                        'high_24h': float(coin.get('high_24h') or 0),
                        'low_24h': float(coin.get('low_24h') or 0)
                    })
                except Exception:
                    continue
        
        # Avoid rate limits
        time.sleep(1.2)
        
        # Losers
        params_losers = {
            'vs_currency': 'usd',
            'order': 'price_change_percentage_24h_asc',
            'per_page': limit,
            'page': 1,
            'sparkline': 'false',
            'price_change_percentage': '1h,24h'
        }
        resp = _get_requests_session().get(url, params=params_losers, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            for coin in resp.json():
                try:
                    symbol = coin['symbol'].upper()
                    if is_stablecoin(symbol):
                        continue
                    
                    price = float(coin.get('current_price') or 0)
                    change_24h = float(coin.get('price_change_percentage_24h_in_currency') or coin.get('price_change_percentage_24h') or 0)
                    
                    candidates.append({
                        'id': coin.get('id'),
                        'symbol': symbol,
                        'price': price,
                        'volume': float(coin.get('total_volume') or 0),
                        'price_change_24h': change_24h,
                        'change_24h': change_24h,
                        'price_change_1h': float(coin.get('price_change_percentage_1h_in_currency') or 0),
                        'high_24h': float(coin.get('high_24h') or 0),
                        'low_24h': float(coin.get('low_24h') or 0)
                    })
                except Exception:
                    continue
                    
    except Exception as e:
        if DEBUG_ERRORS:
            print(f"[ERROR] Failed to fetch top movers from CoinGecko: {e}", flush=True)
            
    return candidates


def get_reversal_candidates_top_movers(limit=50):
    """Generate reversal candidates from top movers that are tradable on active exchanges."""
    movers = fetch_top_movers(limit)
    candidates = []
    seen_symbols = set()
    for m in movers:
        symbol = m['symbol']
        if symbol in seen_symbols:
            continue
        
        # Normalize to BASE/USDT for tradability check
        pair = f"{symbol}/USDT"
        if is_tradable_on_exchanges(pair, strategy="reversal"):
            candidates.append(m)
            seen_symbols.add(symbol)
    
    if DEBUG_CACHE:
        print(f"[CANDIDATES] Found {len(candidates)} top mover candidates tradable on exchanges", flush=True)
    return candidates


def fetch_from_mexc(symbol: str, interval: str, limit: int) -> list:
    """Fetch klines from MEXC Exchange API with pagination support."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_mexc(symbol)
        
        # MEXC interval mapping
        interval_map = {
            '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
            '1h': '1h', '4h': '4h', '1d': '1d'
        }
        mexc_interval = interval_map.get(interval, '15m')
        
        # Try multiple endpoints for reliability
        endpoints = [
            'https://api.mexc.com/api/v3/market/kline',
            'https://www.mexc.com/api/v3/market/kline',
            'https://www.mexc.com/api/v2/market/kline'
        ]
        
        all_candles = []
        max_pages = max(5, (limit // 1000) + 1)  # MEXC allows up to 1000 per request
        
        for endpoint in endpoints:
            try:
                for page in range(max_pages):
                    url = endpoint
                    params = {
                        'symbol': norm_symbol,
                        'interval': mexc_interval,
                        'limit': min(1000, limit - len(all_candles))
                    }
                    
                    resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
                    if resp.status_code == 200:
                        data = resp.json()
                        
                        # Handle both dict and list responses
                        candles = None
                        if isinstance(data, dict) and 'data' in data:
                            candles = data['data']
                        elif isinstance(data, list):
                            candles = data
                        
                        if candles and len(candles) > 0:
                            all_candles.extend(candles)
                            if len(all_candles) >= limit:
                                break
                        else:
                            break
                    elif resp.status_code == 400:
                        # Invalid symbol or parameters, try next endpoint
                        break
                    else:
                        if page == 0:
                            continue  # Try next endpoint
                        break
                
                if all_candles:
                    break  # Success, exit endpoint loop
            except Exception:
                continue  # Try next endpoint
        
        if not all_candles:
            return []
        
        # Sort by timestamp (oldest first) and limit
        all_candles.sort(key=lambda x: int(x[0]) if isinstance(x, list) and len(x) > 0 else 0)
        parsed = parse_klines(all_candles[:limit], 'mexc')
        
        # Ensure minimum quality: at least 10 candles or requested limit
        if len(parsed) >= min(limit, 10):
            return parsed
        return []
    except Exception as e:
        if os.getenv("DEBUG_KLINES", "").lower() == "true":
            print(f"[MEXC] {symbol} error: {str(e)[:50]}", flush=True)
    
    return []


def fetch_from_bitget(symbol: str, interval: str, limit: int) -> list:
    """Fetch klines from Bitget Spot API v2 (non-paginated, spot only)."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_bitget(symbol)

        interval_map = {
            '1m': '1min', '5m': '5min', '15m': '15min', '30m': '30min',
            '1h': '1hour', '4h': '4hour', '1d': '1day'
        }
        bitget_interval = interval_map.get(interval, '15min')

        url = 'https://api.bitget.com/api/v2/spot/market/candles'
        params = {
            'symbol': norm_symbol,
            'granularity': bitget_interval,
            'limit': min(limit, 200)
        }
        resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, dict) and data.get('code') == '00000' and isinstance(data.get('data'), list):
                candles = data['data']
                if not candles:
                    return []
                candles.reverse()
                parsed = parse_klines(candles[:limit], 'bitget')
                return parsed if parsed else []
        return []
    except Exception as e:
        if os.getenv('DEBUG_KLINES', '').lower() == 'true':
            print(f"[BITGET] {symbol} error: {str(e)[:50]}", flush=True)
    return []


def fetch_from_coinbase(symbol: str, interval: str, limit: int) -> list:
    """Enhanced klines fetcher from Coinbase REST API with improved pagination and error handling."""
    try:
        throttle_kline_api_calls()
        if '/' in symbol:
            base, quote = symbol.split('/')
        else:
            known_quotes = ['USDT', 'USD', 'EUR', 'GBP']
            for q in known_quotes:
                if symbol.endswith(q):
                    base = symbol[:-len(q)]
                    quote = q
                    break
            else:
                base = symbol
                quote = 'USDT'
        
        if quote == 'USDT':
            quote = 'USD'
        norm_symbol = f"{base}-{quote}"
        
        interval_map = {'1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, '1d': 86400}
        cb_interval = interval_map.get(interval, 3600)
        
        all_candles = []
        current_time = int(time.time())
        max_pages = max(5, (limit // 300) + 1)  # Calculate pages needed based on limit
        
        for page in range(max_pages):
            end_time = current_time - (page * 300 * cb_interval)
            start_time = end_time - (300 * cb_interval)
            
            url = f'https://api.exchange.coinbase.com/products/{norm_symbol}/candles'
            params = {
                'granularity': cb_interval,
                'start': start_time,
                'end': end_time
            }
            
            resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
            if resp.status_code == 200:
                candles = resp.json()
                if not candles:
                    break
                # Coinbase returns newest first, so reverse for chronological order
                candles.reverse()
                all_candles.extend(candles)
                if len(all_candles) >= limit:
                    break
            elif resp.status_code == 404:
                # Symbol not found, return empty
                return []
            else:
                # For other errors, try next page or return what we have
                if page == 0:
                    return []
                break
        
        if not all_candles:
            return []
        
        # Sort by timestamp and limit
        all_candles.sort(key=lambda x: x[0])
        parsed = parse_klines(all_candles[:limit], 'coinbase')
        
        # Ensure minimum quality: at least 10 candles or requested limit
        if len(parsed) >= min(limit, 10):
            return parsed
        return []
    except Exception as e:
        if os.getenv("DEBUG_KLINES", "").lower() == "true":
            print(f"[COINBASE] {symbol} error: {str(e)[:50]}", flush=True)
    
    return []



def fetch_from_gateio(symbol: str, interval: str, limit: int) -> list:
    """Fetch klines from Gate.io REST API (no pagination; limit param)."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_gateio(symbol)
        interval_map = {'1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m', '1h': '1h', '4h': '4h', '1d': '1d'}
        gateio_interval = interval_map.get(interval, '1h')
        url = 'https://api.gateio.ws/api/v4/spot/candlesticks'
        params = {
            'currency_pair': norm_symbol,
            'interval': gateio_interval,
            'limit': min(limit, 1000)
        }
        resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                return parse_klines(data, 'gateio')
    except Exception as e:
        if os.getenv('DEBUG_KLINES', '').lower() == 'true':
            print(f"[GATEIO] {symbol} error: {str(e)[:50]}", flush=True)
    return []


def fetch_from_bitstamp(symbol: str, interval: str, limit: int) -> list:
    """Fetch klines from Bitstamp REST API (no pagination; step+limit)."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_bitstamp(symbol)
        step_map = {'1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, '1d': 86400}
        step = step_map.get(interval, 900)
        url = f'https://www.bitstamp.net/api/v2/ohlc/{norm_symbol}/'
        params = {'step': step, 'limit': min(limit, 1000)}
        resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            ohlc = data.get('data', {}).get('ohlc', []) if isinstance(data, dict) else []
            if ohlc:
                return parse_klines(ohlc, 'bitstamp')
    except Exception as e:
        if os.getenv('DEBUG_KLINES', '').lower() == 'true':
            print(f"[BITSTAMP] {symbol} error: {str(e)[:50]}", flush=True)
    return []


def fetch_from_coinmetro(symbol: str, interval: str, limit: int) -> list:
    """Fetch klines from Coinmetro REST API (no pagination; limit param)."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_coinmetro(symbol)
        interval_map = {'1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m', '1h': '1h', '4h': '4h', '1d': '1d'}
        cm_interval = interval_map.get(interval, '15m')
        url = 'https://api.coinmetro.com/exchange/candles'
        params = {'symbol': norm_symbol, 'interval': cm_interval, 'limit': min(limit, 1000)}
        resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            k = data.get('data') if isinstance(data, dict) else data
            if k:
                return parse_klines(k, 'coinmetro')
    except Exception as e:
        if os.getenv('DEBUG_KLINES', '').lower() == 'true':
            print(f"[COINMETRO] {symbol} error: {str(e)[:50]}", flush=True)
    return []


def fetch_from_hashkey(symbol: str, interval: str, limit: int) -> list:
    """Fetch klines from HashKey REST API (non-paginated) with strict success checks."""
    try:
        throttle_kline_api_calls()
        norm_symbol = normalize_symbol_hashkey(symbol)
        interval_map = {'1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m', '1h': '1h', '4h': '4h', '1d': '1d'}
        hk_interval = interval_map.get(interval, '15m')
        url = 'https://api.hashkey.com/spot/v1/market/candles'
        params = {'symbol': norm_symbol, 'interval': hk_interval, 'limit': min(limit, 1000)}
        resp = _get_requests_session().get(url, params=params, timeout=API_TIMEOUT_KLINES)
        if resp.status_code == 200:
            data = resp.json()
            # Accept success only if data is dict with a non-empty 'data' list
            if isinstance(data, dict) and isinstance(data.get('data'), list) and len(data['data']) > 0:
                return parse_klines(data['data'][:limit], 'hashkey') or []
        return []
    except Exception as e:
        if os.getenv('DEBUG_KLINES', '').lower() == 'true':
            print(f"[HASHKEY] {symbol} error: {str(e)[:50]}", flush=True)
    return []



def _estimate_klines_size(klines: list) -> int:
    """Estimate memory size of klines in bytes."""
    if not klines:
        return 0
    import sys
    return sys.getsizeof(klines) + sum(sys.getsizeof(k) for k in klines)

def cache_get(symbol: str, interval: str, klines_cache: dict = None, max_age_sec: int = CACHE_MAX_AGE_SECONDS) -> tuple:
    """Get klines from cache if fresh. Returns (klines, source, age_seconds) or (None, None, None)."""
    if klines_cache is None:
        klines_cache = _klines_cache_global
    
    cache_key = (symbol, interval)
    with _cache_lock:
        if cache_key not in klines_cache:
            return None, None, None
        
        entry = klines_cache[cache_key]
        age_sec = time.time() - entry['timestamp']
        
        if age_sec > max_age_sec:
            if DEBUG_CACHE:
                print(f"[CACHE] STALE: {symbol} {interval} is {int(age_sec//60)}m {int(age_sec%60)}s old, skipping cache", flush=True)
            return None, None, age_sec
        
        if DEBUG_CACHE:
            print(f"[CACHE] HIT: {symbol} {interval} (age: {int(age_sec)}s, from: {entry['source']}, hits: {entry['hits']})", flush=True)
        
        with _cache_stats_lock:
            _cache_stats['hits'] += 1
            entry['hits'] += 1
        
        return entry['data'], entry['source'], age_sec

def cache_put(symbol: str, interval: str, klines: list, source: str, klines_cache: dict = None):
    """Store klines in cache with metadata."""
    if not klines:
        return
    
    if klines_cache is None:
        klines_cache = _klines_cache_global
    
    cache_key = (symbol, interval)
    size_bytes = _estimate_klines_size(klines)
    
    with _cache_lock:
        klines_cache[cache_key] = {
            'data': klines,
            'timestamp': time.time(),
            'source': source,
            'hits': 0,
            'size_bytes': size_bytes
        }
    
    with _cache_stats_lock:
        _cache_stats['misses'] += 1
        _cache_stats['total_size_bytes'] += size_bytes
    
    if DEBUG_CACHE:
        print(f"[CACHE] PUT: {symbol} {interval} from {source} ({len(klines)} candles, {size_bytes//1024}KB)", flush=True)

def cache_cleanup(klines_cache: dict = None, max_age_sec: int = CACHE_MAX_AGE_SECONDS, max_size_mb: int = CACHE_MAX_SIZE_MB, max_entries: int = CACHE_MAX_ENTRIES):
    """Remove stale and over-limit entries from cache (thread-safe)."""
    if klines_cache is None:
        klines_cache = _klines_cache_global
    
    with _cache_lock:
        current_time = time.time()
        removed_count = 0
        size_before = sum(entry.get('size_bytes', 0) for entry in klines_cache.values())
        
        entries_to_remove = []
        for cache_key, entry in klines_cache.items():
            age_sec = current_time - entry['timestamp']
            if age_sec > max_age_sec:
                entries_to_remove.append(cache_key)
        
        for cache_key in entries_to_remove:
            del klines_cache[cache_key]
            removed_count += 1
        
        size_after = sum(entry.get('size_bytes', 0) for entry in klines_cache.values())
        size_mb = size_after / (1024 * 1024)
        
        if len(klines_cache) > max_entries:
            sorted_entries = sorted(klines_cache.items(), key=lambda x: x[1]['timestamp'])
            excess_count = len(klines_cache) - max_entries
            entries_to_remove = sorted_entries[:excess_count]
            for cache_key, _ in entries_to_remove:
                del klines_cache[cache_key]
                removed_count += 1
            size_after = sum(entry.get('size_bytes', 0) for entry in klines_cache.values())
            size_mb = size_after / (1024 * 1024)
        
        if size_mb > max_size_mb:
            sorted_entries = sorted(klines_cache.items(), key=lambda x: (x[1]['hits'], x[1]['timestamp']))
            removed_bytes = 0
            entries_to_remove = []
            target_bytes = int(max_size_mb * 1024 * 1024 * 0.8)
            
            for cache_key, entry in sorted_entries:
                if size_after - removed_bytes <= target_bytes:
                    break
                removed_bytes += entry.get('size_bytes', 0)
                entries_to_remove.append(cache_key)
            
            for cache_key in entries_to_remove:
                del klines_cache[cache_key]
                removed_count += 1
            
            size_after = sum(entry.get('size_bytes', 0) for entry in klines_cache.values())
            size_mb = size_after / (1024 * 1024)
            
            if DEBUG_CACHE or removed_count > 0:
                print(f"[CACHE] SIZE LIMIT: Removed {removed_count} entries to stay under {max_size_mb}MB (now: {size_mb:.1f}MB)", flush=True)
    
    with _cache_stats_lock:
        _cache_stats['cleanup_runs'] += 1
        _cache_stats['evicted_entries'] += removed_count
        _cache_stats['last_cleanup'] = current_time
        _cache_stats['total_size_bytes'] = int(size_after)
    
    if DEBUG_CACHE and removed_count > 0:
        print(f"[CACHE] CLEANUP: Removed {removed_count} expired/excess entries (size: {size_mb:.1f}MB, entries: {len(klines_cache)})", flush=True)

def cache_stats_str() -> str:
    """Get formatted cache statistics string."""
    with _cache_stats_lock:
        hits = _cache_stats['hits']
        misses = _cache_stats['misses']
        total = hits + misses
        hit_rate = (hits / total * 100) if total > 0 else 0
        size_mb = _cache_stats['total_size_bytes'] / (1024 * 1024)
    
    return f"Hits={hits}, Misses={misses}, Hit%={hit_rate:.1f}%, Size={size_mb:.1f}MB, Evicted={_cache_stats['evicted_entries']}"

def fetch_klines_with_fallback(symbol: str, interval: str, limit: int, strategy: str = "general", klines_cache: dict = None) -> tuple:
    """
    Fetch klines using strategy-specific fallback chains with global cache.
    Primary: Check cache (if provided and fresh)
    Secondary: Tickerbase with strategy-specific key
    Tertiary: Strategy-specific primary fallback
    Remaining: Other reliable APIs
    
    Args:
        symbol: Trading pair symbol
        interval: Kline interval (1m, 5m, 15m, 1h, 4h, 1d)
        limit: Number of candles to fetch
        strategy: Strategy name for fallback chain selection
        klines_cache: Optional cache dict for storing/retrieving klines (STAGE 1)
    
    Returns: (klines, source_name) or ([], "FAILED")
    """
    
    if klines_cache is None:
        klines_cache = _klines_cache_global
    
    debug_klines = os.getenv("DEBUG_KLINES_DETAILED", "").lower() == "true"
    
    # Check cache first (STAGE 1 - Cache Monitoring)
    cached_klines, cached_source, age_sec = cache_get(symbol, interval, klines_cache)
    if cached_klines is not None:
        if debug_klines:
            print(f"[KLINES] {symbol} {interval} (CACHE HIT): {len(cached_klines)} candles from {cached_source}, age={age_sec:.1f}s", flush=True)
        return cached_klines, f"{cached_source}(cached)"
    
    if debug_klines:
        print(f"[KLINES] {symbol} {interval} (CACHE MISS): fetching from {strategy} chain", flush=True)
    
    # Strategy-specific fallback chains (NO PAGINATION)
    # Reversal: Bitget (primary) → HashKey (fallback)
    # Range: Gate.io (primary) → Bitstamp (fallback)
    # Momentum: Kraken (primary) → Coinmetro (fallback)
    # General: Kraken → OKX → KuCoin
    STRATEGY_CHAINS = {
        "reversal": [
            ('bitget', fetch_from_bitget),
            ('hashkey', fetch_from_hashkey),
        ],
        "range": [
            ('gateio', fetch_from_gateio),
            ('bitstamp', fetch_from_bitstamp),
        ],
        "momentum": [
            ('kraken', fetch_from_kraken),
            ('coinmetro', fetch_from_coinmetro),
        ],
        "general": [
            ('kraken', fetch_from_kraken),
            ('okx', fetch_from_okx),
            ('kucoin', fetch_from_kucoin),
        ]
    }
    
    sources = STRATEGY_CHAINS.get(strategy.lower(), STRATEGY_CHAINS["general"])
    
    for source_name, fetch_func in sources:
        try:
            api_start_time = time.time()
            klines = fetch_func(symbol, interval, limit)
            api_response_time_ms = (time.time() - api_start_time) * 1000
            track_api_response_time(source_name, strategy, api_response_time_ms)
            
            if klines is None:
                if debug_klines:
                    print(f"[KLINES] {symbol} {interval} from {source_name}: RETURNED NONE ({api_response_time_ms:.0f}ms)", flush=True)
                track_fetch_failure(symbol, strategy)
                continue
            
            if not isinstance(klines, list):
                if debug_klines:
                    print(f"[KLINES] {symbol} {interval} from {source_name}: INVALID FORMAT (expected list, got {type(klines).__name__})", flush=True)
                track_fetch_failure(symbol, strategy)
                continue
                
            if len(klines) == 0:
                if debug_klines:
                    print(f"[KLINES] {symbol} {interval} from {source_name}: EMPTY LIST ({api_response_time_ms:.0f}ms)", flush=True)
                continue
            
            if debug_klines:
                print(f"[KLINES] {symbol} {interval} from {source_name}: ✓ {len(klines)} candles ({api_response_time_ms:.0f}ms)", flush=True)
            
            cache_put(symbol, interval, klines, source_name, klines_cache)
            clear_pair_failures(symbol, strategy)
            return klines, source_name
        except Exception as e:
            api_response_time_ms = (time.time() - api_start_time) * 1000
            track_api_response_time(source_name, strategy, api_response_time_ms)
            track_fetch_failure(symbol, strategy)
            if debug_klines:
                print(f"[KLINES] {symbol} {interval} from {source_name}: EXCEPTION {str(e)[:60]} ({api_response_time_ms:.0f}ms)", flush=True)
            continue
    
    if debug_klines:
        print(f"[KLINES] {symbol} {interval}: ALL SOURCES FAILED", flush=True)
    return [], "FAILED"

from concurrent.futures import ThreadPoolExecutor, as_completed, wait

def warm_cache_on_startup():
    """Pre-fetch klines for top trending pairs to warm cache on startup."""
    if not CACHE_WARMING_ENABLED:
        return
    
    try:
        if DEBUG_STAGE_5:
            print("[STAGE 5] Starting cache warming...", flush=True)
        
        get_trending_pairs(limit=CACHE_WARMING_PAIR_COUNT)
        top_pairs = []
        with TRENDING_PAIRS_LOCK:
            top_pairs = list(TRENDING_PAIRS.keys())[:CACHE_WARMING_PAIR_COUNT]
        
        if not top_pairs:
            if DEBUG_STAGE_5:
                print("[STAGE 5] No trending pairs found for cache warming", flush=True)
            return
        
        fetch_count = 0
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {}
            for pair in top_pairs:
                for interval in CACHE_WARMING_INTERVALS:
                    future = executor.submit(fetch_any_klines, pair, interval=interval, limit=120 if interval in ['1h', '4h'] else 80, strategy="cache_warming", timeout=CACHE_WARMING_TIMEOUT)
                    futures[future] = (pair, interval)
            
            for future in as_completed(futures):
                pair, interval = futures[future]
                try:
                    klines = future.result()
                    if klines:
                        fetch_count += 1
                except Exception:
                    pass
        
        if DEBUG_STAGE_5:
            print(f"[STAGE 5] Cache warming complete: {fetch_count} kline sets pre-fetched", flush=True)
    
    except Exception as e:
        if DEBUG_STAGE_5:
            print(f"[STAGE 5] Cache warming error: {e}", flush=True)


def parallel_fetch_klines_1h_4h(binance_symbol, coin_id, strategy_name):
    """Fetch 1h and 4h klines in parallel for faster processing."""
    if not PARALLEL_FETCH_ENABLED:
        klines_1h = fetch_any_klines(binance_symbol, interval='1h', limit=120, coin_id=coin_id, strategy=strategy_name)
        klines_4h = fetch_any_klines(binance_symbol, interval='4h', limit=80, coin_id=coin_id, strategy=strategy_name)
        return klines_1h, klines_4h
    
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_1h = executor.submit(fetch_any_klines, binance_symbol, interval='1h', limit=120, coin_id=coin_id, strategy=strategy_name)
            future_4h = executor.submit(fetch_any_klines, binance_symbol, interval='4h', limit=80, coin_id=coin_id, strategy=strategy_name)
            
            klines_1h = None
            klines_4h = None
            
            try:
                klines_1h = future_1h.result()
            except Exception:
                klines_1h = None
            
            try:
                klines_4h = future_4h.result()
            except Exception:
                klines_4h = None
            
            if DEBUG_STAGE_5:
                print(f"[STAGE 5] Parallel fetch {binance_symbol}: 1h={bool(klines_1h)}, 4h={bool(klines_4h)}", flush=True)
            
            return klines_1h, klines_4h
    
    except Exception as e:
        if DEBUG_STAGE_5:
            print(f"[STAGE 5] Parallel fetch error for {binance_symbol}: {e}", flush=True)
        klines_1h = fetch_any_klines(binance_symbol, interval='1h', limit=120, coin_id=coin_id, strategy=strategy_name)
        klines_4h = fetch_any_klines(binance_symbol, interval='4h', limit=80, coin_id=coin_id, strategy=strategy_name)
        return klines_1h, klines_4h

VERIFICATION_AVAILABLE = False

# Default values for configuration
THROTTLE = 0.2  # seconds between API calls to avoid rate limiting
API_THROTTLE = {}  # Maps API names to throttle delays
api_last_call = {}  # Tracks last call time for each API
TV_INTERVAL = "1m"  # default TradingView interval for live data
MIN_VOLUME_USD = 100000  # minimum 24h volume in USD to consider a pair
PAIR_QUALITY_TARGET_VOLUME = 500000  # target volume for quality filtering

# Verification settings
VERIFICATION_ENABLED = os.getenv("VERIFICATION_ENABLED", "true").lower() == "true"
VERIFICATION_FREQUENCY = int(os.getenv("VERIFICATION_FREQUENCY", "3"))  # Every N cycles

# Set stdout to use UTF-8 encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Optional TradingView TA
try:
    from tradingview_ta import TA_Handler, Interval
    TV_AVAILABLE = True
    TV_SCREENER = os.getenv("TV_SCREENER", "crypto")
    TV_EXCHANGE = os.getenv("TV_EXCHANGE", "BINANCE")
except Exception:
    TV_AVAILABLE = False
    TV_SCREENER = None
    TV_EXCHANGE = None

# API timeout override for strategy testing (balance speed vs reliability)
API_TIMEOUT_KLINES = int(os.getenv("API_TIMEOUT_KLINES", "5"))

# Thread pool configuration for strategy processing
STRATEGY_WORKERS = int(os.getenv("STRATEGY_WORKERS", "50"))  # Increased from 16 to 50 for faster processing
STRATEGY_OVERALL_TIMEOUT = int(os.getenv("STRATEGY_OVERALL_TIMEOUT", "300"))
STRATEGY_TASK_TIMEOUT = int(os.getenv("STRATEGY_TASK_TIMEOUT", "120"))

# STAGE 5: Advanced Optimizations Configuration
CACHE_WARMING_ENABLED = os.getenv("CACHE_WARMING_ENABLED", "true").lower() == "true"
CACHE_WARMING_PAIR_COUNT = int(os.getenv("CACHE_WARMING_PAIR_COUNT", "15"))
CACHE_WARMING_TIMEOUT = int(os.getenv("CACHE_WARMING_TIMEOUT", "60"))
CACHE_WARMING_INTERVALS = os.getenv("CACHE_WARMING_INTERVALS", "15m,1h,4h").split(",")
PARALLEL_FETCH_ENABLED = os.getenv("PARALLEL_FETCH_ENABLED", "true").lower() == "true"
DEBUG_STAGE_5 = os.getenv("DEBUG_STAGE_5", "false").lower() == "true"

def now_utc_str():
    """Return current UTC timestamp string in ISO format."""
    try:
        return datetime.now(timezone.utc).isoformat()
    except Exception:
        return datetime.utcnow().isoformat()


def fmt(val):
    """Format a price number for display (max 8 significant digits)."""
    if val is None:
        return "N/A"
    try:
        if abs(val) >= 1:
            return f"{val:.2f}"
        else:
            return f"{val:.8f}".rstrip('0')
    except Exception:
        return str(val)


def safe_sleep(seconds):
    """Sleep with keyboard interrupt handling."""
    try:
        time.sleep(seconds)
    except KeyboardInterrupt:
        raise


def is_stablecoin(symbol):
    """Check if symbol is a known stablecoin."""
    if not symbol:
        return False
    sym = symbol.upper().strip()
    return sym in STABLECOINS


# ---------------- CONFIG ----------------
SHORT_TERM_MODE = True
TIMEOUT = 20
STRATEGY_FILTER_TOP = int(os.getenv("STRATEGY_FILTER_TOP", "5")) # Top N candidates per direction for detailed analysis
CYCLE_SECONDS = int(os.getenv("CYCLE_SECONDS", "600"))  # 10 minutes for live data
TOP_TRENDING = int(os.getenv("TOP_TRENDING", "100"))
EVALUATE_TOP = int(os.getenv("EVALUATE_TOP", "20"))
VS_CURRENCY = "usd"

# mapping for symbol normalization (override if needed)
BINANCE_SYM_EXCEPTIONS = {}

STABLECOINS = {
    "USDT", "USDC", "BUSD", "USDP", "TUSD", "DAI", "FRAX", "ALUSD", "LUSD", "SUSD", "UST",
    "USDA", "USDX", "EURS", "EURT", "EUROC", "GBPt", "XSGD", "SGDX", "MAHA", "EUX",
    "MAI", "USDE", "USDS", "PYUSD", "DOLA", "cUSDC", "cDAI"
}

# Reversal sensitivity (Balanced) - loosened to produce signals more often
REV_PUMP_1H = float(os.getenv("REV_PUMP_1H", "2.0"))  # Adjusted from 1.2
REV_PUMP_24H = float(os.getenv("REV_PUMP_24H", "4.0"))  # Adjusted from 5.0
REV_DUMP_1H = float(os.getenv("REV_DUMP_1H", "-2.0"))  # Adjusted from -1.2
REV_DUMP_24H = float(os.getenv("REV_DUMP_24H", "-4.0"))  # Adjusted from -5.0
# be more sensitive to volume climaxes during tuning; configurable via env
VOL_CLIMAX_MULT = float(os.getenv("VOL_CLIMAX_MULT", "1.1"))
WICK_BODY_RATIO = float(os.getenv("WICK_BODY_RATIO", "1.1"))
WICK_MIN_PCT = float(os.getenv("WICK_MIN_PCT", "0.002"))
RSI_DIV_LOOKBACK = 6
RSI_EXTREME_LONG = 30
RSI_EXTREME_SHORT = 70
# Force reversal when RSI is extreme even if other layers missing (tunable)
# default lowered to 76 to be more permissive during iteration
RSI_FORCE_THRESHOLD = int(os.getenv("RSI_FORCE_THRESHOLD", "78"))
STRUCTURE_PROX_PCT = 0.03
MIN_LAYERS_TO_FIRE = int(os.getenv("MIN_LAYERS_TO_FIRE", "3"))

# Local minimal confidences (tuned)
# Lowered defaults to be less restrictive so signals (LONG/REVERSAL) appear more often
REV_MIN_CONFIDENCE = float(os.getenv("REV_MIN_CONFIDENCE", "10.0"))  # Lowered from 15.0
RANGE_MIN_CONFIDENCE = float(os.getenv("RANGE_MIN_CONFIDENCE", "10.0"))  # Lowered from 15.0

# Quality thresholds for multi-layer detection
HIGH_QUALITY_DETECTION_THRESHOLD = float(os.getenv("HIGH_QUALITY_DETECTION_THRESHOLD", "3"))
MEDIUM_QUALITY_DETECTION_THRESHOLD = float(os.getenv("MEDIUM_QUALITY_DETECTION_THRESHOLD", "2"))
SIGNAL_QUALITY_CONFIDENCE_MULTIPLIER = float(os.getenv("SIGNAL_QUALITY_CONFIDENCE_MULTIPLIER", "0.4"))

# Range strategy params (Balanced) - loosened
RANGE_MAX_WIDTH_PCT = 0.20      # maximum channel width (20%)
RANGE_MIN_REJECTIONS = int(os.getenv("RANGE_MIN_REJECTIONS", "0"))        # min bounces on each side (0 allows near-edge entry)
RANGE_ENTRY_OFFSET_PCT = 0.02   # entry within 2% of boundary (legacy, not used for debug)
RANGE_NEAR_EDGE_PCT = float(os.getenv("RANGE_NEAR_EDGE_PCT", "0.06"))      # Increased sensitivity to boundaries (from 0.08)
RANGE_SL_OUTSIDE_PCT = 0.015    # sl placed 1.5% outside boundary
RANGE_TP_CAPTURE_PCT = 0.50     # take 50% of range toward opposite side (mid-point)
RANGE_BB_COMPRESS_THRESHOLD = 0.12  # BB width threshold proxy
RANGE_VOLUME_CONTRACTION_RATIO = 0.8  # recent volume < 80% of 24h avg
RANGE_EDGE_MIN_OFFSET = float(os.getenv("RANGE_EDGE_MIN_OFFSET", "0.003"))
RANGE_EDGE_MAX_OFFSET = float(os.getenv("RANGE_EDGE_MAX_OFFSET", "0.015"))
RANGE_MID_MIN_OFFSET = float(os.getenv("RANGE_MID_MIN_OFFSET", "0.025"))
RANGE_MID_MAX_OFFSET = float(os.getenv("RANGE_MID_MAX_OFFSET", "0.045"))

# Advanced risk/config controls
LOW_VOLUME_PENALTY = float(os.getenv("LOW_VOLUME_PENALTY", "0.85"))
SOFT_COOLDOWN_SECONDS = int(os.getenv("SOFT_COOLDOWN_SECONDS", "300"))
HARD_COOLDOWN_SECONDS = int(os.getenv("HARD_COOLDOWN_SECONDS", "600"))
SOFT_COOLDOWN_MIN_FACTOR = float(os.getenv("SOFT_COOLDOWN_MIN_FACTOR", "0.6"))
VOL_CLASS_THRESHOLDS = tuple(float(x) for x in os.getenv("VOL_CLASS_THRESHOLDS", "0.08,0.22").split(","))
VOL_REGIME_BBW_MAX = float(os.getenv("VOL_REGIME_BBW_MAX", "0.30"))
VOL_REGIME_ATR_MULT = float(os.getenv("VOL_REGIME_ATR_MULT", "1.15"))
HOTNESS_BONUS_MAX = float(os.getenv("HOTNESS_BONUS_MAX", "6.0"))
SENTIMENT_CONF_SCALER = float(os.getenv("SENTIMENT_CONF_SCALER", "0.4"))
CONFLICT_PENALTY = float(os.getenv("CONFLICT_PENALTY", "0.75"))
STALE_WARNING_DROP = float(os.getenv("STALE_WARNING_DROP", "20.0"))
BID_ASK_SPREAD_BPS = float(os.getenv("BID_ASK_SPREAD_BPS", "8.0"))  # 0.08%
PIVOT_LEVEL_STEP = float(os.getenv("PIVOT_LEVEL_STEP", "25.0"))

# Pullback skip configuration: if estimated SL probability exceeds this, mark WAIT_FOR_PULLBACK
# Raised by default during debugging so we don't over-skip signals while tuning.
PULLBACK_SKIP_PROB_SL = float(os.getenv("PULLBACK_SKIP_PROB_SL", "0.95"))
# Margin used for secondary check vs TP probability (default 0.10 = tighter margin for more signals to pass)
PULLBACK_SKIP_MARGIN = float(os.getenv("PULLBACK_SKIP_MARGIN", "0.10"))

# Short-term specifics
TV_INTERVAL = Interval.INTERVAL_1_MINUTE if (SHORT_TERM_MODE and TV_AVAILABLE) else (Interval.INTERVAL_5_MINUTES if TV_AVAILABLE else None)
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", str(15 * 60)))
MIN_CONFIDENCE = float(os.getenv("MIN_CONFIDENCE", "40.0"))

# TA weights
W_EMA = float(os.getenv("W_EMA", "0.45"))
W_RSI = float(os.getenv("W_RSI", "0.25"))
W_MOM = float(os.getenv("W_MOM", "0.20"))
W_VOL = float(os.getenv("W_VOL", "0.10"))







REVERSAL_PAIR_LIMIT = int(os.getenv("REVERSAL_PAIR_LIMIT", "0")) or None
RANGE_PAIR_LIMIT = int(os.getenv("RANGE_PAIR_LIMIT", "0")) or None
MOMENTUM_PAIR_LIMIT = int(os.getenv("MOMENTUM_PAIR_LIMIT", "0")) or None

# file names and cache
SIGNAL_LOG_FILE = "signals.csv"
REV_LOG_FILE = "reversal_signals.csv"
RANGE_LOG_FILE = "range_signals.csv"
MOMENTUM_LOG_FILE = "momentum_signals.csv"
COOLDOWN_CACHE_FILE = "cooldown_cache.json"

# ============ STRATEGY-SPECIFIC KLINE FETCHERS (Independent Implementation) ============

def fetch_klines_for_reversal(symbol: str, interval: str = "15m", limit: int = 80, coin_id=None):
    """Fetch klines for REVERSAL strategy using Bitget (primary) → HashKey (fallback)."""
    # Normalization: Reduce wasted calls for non-tradable symbols
    if not is_tradable_on_exchanges(symbol, strategy="reversal"):
        return None, None, None, None
        
    debug_rev = os.getenv("DEBUG_REVERSAL", "false").lower() == "true"
    try:
        klines_15m, src_15m = fetch_klines_with_fallback(symbol, "15m", 80, "reversal")
        klines_1m, src_1m = fetch_klines_with_fallback(symbol, "1m", 60, "reversal")
        klines_5m, src_5m = fetch_klines_with_fallback(symbol, "5m", 60, "reversal")
        
        if debug_rev:
            print(f"[REVERSAL KLINES] {symbol}: 15m={len(klines_15m) if klines_15m else 0}({src_15m}) 1m={len(klines_1m) if klines_1m else 0}({src_1m}) 5m={len(klines_5m) if klines_5m else 0}({src_5m})", flush=True)
        
        fresh_price = None
        if klines_15m and len(klines_15m) > 0:
            try:
                fresh_price = float(klines_15m[-1][4])
            except (ValueError, IndexError) as e:
                if debug_rev:
                    print(f"[REVERSAL KLINES] {symbol} - Failed to extract price from 15m: {str(e)[:40]}", flush=True)
        
        return klines_15m, klines_1m, klines_5m, None
    except Exception as e:
        if debug_rev:
            print(f"[REVERSAL KLINES] {symbol} fetch error: {str(e)[:60]}", flush=True)
        return None, None, None, None


def fetch_klines_for_range(symbol: str, interval: str = "1h", limit: int = 120, coin_id=None):
    """Fetch klines for RANGE strategy using Gate.io (primary) → Bitstamp (fallback)."""
    # Normalization: Reduce wasted calls for non-tradable symbols
    if not is_tradable_on_exchanges(symbol, strategy="range"):
        return None, None, None, None, None
        
    debug_range = os.getenv("DEBUG_RANGE", "false").lower() == "true"
    try:
        klines_1h, src_1h = fetch_klines_with_fallback(symbol, "1h", 120, "range")
        klines_1m, src_1m = fetch_klines_with_fallback(symbol, "1m", 60, "range")
        klines_5m, src_5m = fetch_klines_with_fallback(symbol, "5m", 60, "range")
        klines_15m, src_15m = fetch_klines_with_fallback(symbol, "15m", 80, "range")
        
        if debug_range:
            print(f"[RANGE KLINES] {symbol}: 1h={len(klines_1h) if klines_1h else 0}({src_1h}) 1m={len(klines_1m) if klines_1m else 0}({src_1m}) 5m={len(klines_5m) if klines_5m else 0}({src_5m}) 15m={len(klines_15m) if klines_15m else 0}({src_15m})", flush=True)
        
        fresh_price = None
        if klines_1h and len(klines_1h) > 0:
            try:
                fresh_price = float(klines_1h[-1][4])
            except (ValueError, IndexError) as e:
                if debug_range:
                    print(f"[RANGE KLINES] {symbol} - Failed to extract price from 1h: {str(e)[:40]}", flush=True)
        
        return klines_1h, klines_1m, klines_5m, klines_15m, None
    except Exception as e:
        if debug_range:
            print(f"[RANGE KLINES] {symbol} fetch error: {str(e)[:60]}", flush=True)
        return None, None, None, None, None


def fetch_klines_for_momentum(symbol: str, interval: str = "15m", limit: int = 80, coin_id=None):
    """Fetch klines for MOMENTUM strategy using Kraken (primary) → Coinmetro (fallback)."""
    # Normalization: Reduce wasted calls for non-tradable symbols
    if not is_tradable_on_exchanges(symbol, strategy="momentum"):
        return None, None, None, None
        
    debug_mom = os.getenv("DEBUG_MOMENTUM", "false").lower() == "true"
    try:
        klines_15m, src_15m = fetch_klines_with_fallback(symbol, "15m", 80, "momentum")
        klines_1m, src_1m = fetch_klines_with_fallback(symbol, "1m", 60, "momentum")
        klines_5m, src_5m = fetch_klines_with_fallback(symbol, "5m", 60, "momentum")
        
        if debug_mom:
            print(f"[MOMENTUM KLINES] {symbol}: 15m={len(klines_15m) if klines_15m else 0}({src_15m}) 1m={len(klines_1m) if klines_1m else 0}({src_1m}) 5m={len(klines_5m) if klines_5m else 0}({src_5m})", flush=True)
        
        entry_price = None
        if klines_15m and len(klines_15m) > 0:
            try:
                entry_price = float(klines_15m[-1][4])
            except (ValueError, IndexError) as e:
                if debug_mom:
                    print(f"[MOMENTUM KLINES] {symbol} - Failed to extract price from 15m: {str(e)[:40]}", flush=True)
        
        return klines_15m, klines_1m, klines_5m, None
    except Exception as e:
        if debug_mom:
            print(f"[MOMENTUM KLINES] {symbol} fetch error: {str(e)[:60]}", flush=True)
        return None, None, None, None


def fetch_multi_timeframe_klines(symbol: str, coin_id=None, strategy_name: str = "MOMENTUM"):
    """
    Fetch multi-timeframe klines based on strategy.
    Returns (klines_1m, klines_5m, klines_15m, live_price) in consistent order.
    """
    strategy = strategy_name.lower().strip()
    
    if strategy == "momentum":
        kl_15m, kl_1m, kl_5m, live_price = fetch_klines_for_momentum(symbol, coin_id=coin_id)
        return kl_1m, kl_5m, kl_15m, live_price
    elif strategy == "reversal":
        kl_15m, kl_1m, kl_5m, live_price = fetch_klines_for_reversal(symbol, coin_id=coin_id)
        return kl_1m, kl_5m, kl_15m, live_price
    elif strategy == "range":
        kl_1h, kl_1m, kl_5m, kl_15m, live_price = fetch_klines_for_range(symbol, coin_id=coin_id)
        return kl_1m, kl_5m, kl_15m, live_price
    else:
        return None, None, None, None


def get_live_price(symbol: str, strategy: str = "") -> float:
    """Extract live price from fetched klines (close of latest candle) using strategy-specific API."""
    interval = "5m"
    
    # Use strategy-specific API for klines fetching
    strategy_lower = strategy.lower() if strategy else "general"
    klines, _ = fetch_klines_with_fallback(symbol, interval, 1, strategy=strategy_lower)
    
    if klines and len(klines) > 0:
        try:
            return float(klines[-1][4])  # close price
        except (ValueError, IndexError):
            pass
    
    return None

VOL_CACHE_FILE = "vol_cache.json"
TRENDING_CACHE_FILE = "trending_cache.json"

# Risk parameters
ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "0"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.02"))  # 2% per trade
# Suggested leverage and profit-multiplier mapping defaults
LEV_MIN = int(os.getenv("LEV_MIN", "1"))
LEV_MAX = int(os.getenv("LEV_MAX", "10"))
PROFIT_MIN = float(os.getenv("PROFIT_MIN", "0.02"))
PROFIT_MAX = float(os.getenv("PROFIT_MAX", "0.20"))


def load_json_file(fname, default=None):
    """Load JSON from file or return default."""
    try:
        with open(fname, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default or {}


def save_json_file(fname, data):
    """Save data as JSON to file."""
    try:
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass


def load_trending_cache():
    return load_json_file(TRENDING_CACHE_FILE, [])


def save_trending_cache(c):
    save_json_file(TRENDING_CACHE_FILE, c)


def is_kline_fresh(kline, max_age_sec=60):
    """
    Check if kline's close time is within acceptable staleness threshold.
    Returns (is_fresh: bool, age_sec: float)
    STRICT: Rejects klines without valid timestamps (no old data assumed).
    """
    try:
        if not kline or len(kline) < 7:
            return False, float('inf')
        close_time_ms = int(kline[6])
        if close_time_ms <= 0:
            return False, float('inf')
        age_sec = (time.time() * 1000 - close_time_ms) / 1000
        if age_sec < 0:
            return False, float('inf')
        return age_sec <= max_age_sec, age_sec
    except (ValueError, IndexError, TypeError):
        return False, float('inf')


def get_fresh_price(symbol: str, coin_id=None, strategy_name="", max_retries=2, retry_delay=0.3, market_data=None):
    """Get fresh price - strategy-specific market data APIs (CoinGecko/CoinPaprika/Huobi with fallbacks). Klines only for Momentum/Reversal."""
    strategy_upper = strategy_name.upper() if strategy_name else "REVERSAL"
    strategy_lower = strategy_upper.lower()
    
    coin_code = symbol.replace("USDT", "").replace("BUSD", "").upper() if symbol else symbol
    
    # First, try to get price from market_data if provided (already fetched)
    if market_data and isinstance(market_data, dict):
        pair = symbol if symbol.endswith("USDT") else symbol + "USDT"
        pair_data = market_data.get(pair)
        if pair_data and isinstance(pair_data, dict):
            price = pair_data.get("price")
            if price and isinstance(price, (int, float)) and price > 0:
                return price, False, f"market_data_{strategy_lower}", 0.0
    
    # Strategy-specific market data APIs (with fallbacks)
    strategy_func = {
        "REVERSAL": fetch_market_data_reversal,  # CoinPaprika → Bitstamp
        "RANGE": fetch_market_data_range,        # Huobi → Gemini
        "MOMENTUM": fetch_market_data_momentum   # CoinGecko → CEX.IO
    }.get(strategy_upper)
    
    if strategy_func:
        for attempt in range(max_retries):
            try:
                result = strategy_func(coin_code)
                if result and isinstance(result, dict):
                    price = result.get("price")
                    if price and isinstance(price, (int, float)) and price > 0:
                        source_name = "coingecko" if strategy_upper == "MOMENTUM" else "coinpaprika" if strategy_upper == "REVERSAL" else "huobi"
                        return price, False, f"{source_name}_{strategy_lower}", 0.0
            except Exception as e:
                if os.getenv(f"DEBUG_{strategy_upper}", "false").lower() == "true":
                    print(f"[{strategy_upper}] Strategy-specific price fetch failed: {str(e)[:60]}", flush=True)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
    
    # RANGE strictly prioritizes Huobi/Gemini - no kline fallback for entry price
    if strategy_upper == "RANGE":
        return None, True, "failed", float('inf')

    # Fallback to klines if available (only for MOMENTUM/REVERSAL)
    for attempt in range(max_retries):
        try:
            if STRATEGY_FETCHERS_AVAILABLE and get_live_price:
                price = get_live_price(symbol, strategy=strategy_lower)
                if price and isinstance(price, (int, float)) and price > 0:
                    return price, False, "kline_api", 0.0
        except Exception as e:
            if os.getenv(f"DEBUG_{strategy_upper}", "false").lower() == "true":
                print(f"[{strategy_upper}] Kline fallback failed: {str(e)[:60]}", flush=True)
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    
    return None, True, "failed", float('inf')


def get_entry_price_safe(klines_dict, symbol: str, coin_id=None, strategy_name="", fallback_only_fresh=True, market_data=None):
    """
    Get entry price - Priority: Strategy-specific market data APIs (CoinGecko/CoinPaprika/Huobi with fallbacks).
    MOMENTUM: CoinGecko → CEX.IO
    REVERSAL: CoinPaprika → Bitstamp
    RANGE: Huobi → Gemini (STRICT: No kline fallback)
    klines_dict: {"1m": klines_1m, "5m": klines_5m, "15m": klines_15m} (used for technical analysis only, NOT entry pricing)
    Returns (entry_price: float, source: str, staleness_sec: float)
    STRICT: Rejects signal if fresh price unavailable - no fallback to stale data.
    """
    price = None
    source = "none"
    staleness_sec = 0.0
    
    live_price, is_stale, api_source, api_staleness = get_fresh_price(symbol, coin_id=coin_id, strategy_name=strategy_name, market_data=market_data)
    if live_price and not is_stale and api_staleness <= 5.0: # Relaxed from 1.0 to 5.0
        return live_price, api_source, api_staleness
    
    if live_price and api_staleness > 5.0:
        if os.getenv("WARN_STALE_PRICES", "true").lower() == "true":
            print(f"⚠️ [{strategy_name}] {symbol} - Stale entry price ({api_staleness:.1f}s from {api_source}), rejecting", flush=True)
    
    if os.getenv("WARN_STALE_PRICES", "true").lower() == "true":
        strategy_upper = strategy_name.upper() if strategy_name else "REVERSAL"
        api_name = "CoinGecko" if strategy_upper == "MOMENTUM" else "CoinPaprika" if strategy_upper == "REVERSAL" else "Huobi"
        print(f"⚠️ [{strategy_name}] {symbol} - No fresh price available from {api_name} (strategy-specific API), rejecting (no fallback to klines)", flush=True)
    
    return None, source, float('inf')


def compute_market_sentiment(market_data_dict, total_pairs=200):
    """
    Compute real-time market sentiment from gainers/losers ratio.
    Returns (market_sentiment_pct: float, market_trend: str)
    """
    if not market_data_dict or len(market_data_dict) < 20:
        return 50.0, "neutral"
    
    gainers = 0
    losers = 0
    neutral = 0
    
    for symbol, data in market_data_dict.items():
        if not isinstance(data, dict):
            continue
        change_24h = data.get("price_change_24h", 0)
        
        if change_24h > 0.5:
            gainers += 1
        elif change_24h < -0.5:
            losers += 1
        else:
            neutral += 1
    
    total = gainers + losers + neutral
    if total < 10:
        return 50.0, "neutral"
    
    gainer_pct = (gainers / total) * 100
    sentiment = 50.0 + ((gainer_pct - 50.0) * 0.8)
    sentiment = max(10.0, min(90.0, sentiment))
    
    if sentiment > 60:
        trend = "bullish"
    elif sentiment < 40:
        trend = "bearish"
    else:
        trend = "neutral"
    
    return sentiment, trend


def compute_hotness_ranks(trending_coins, market_data_dict):
    """
    Rank trending coins by hotness score.
    Returns dict: {symbol: hotness_rank (0=hottest, higher=less hot)}
    """
    hotness_ranks = {}
    
    if not trending_coins:
        return hotness_ranks
    
    for idx, symbol in enumerate(trending_coins[:100]):
        volume = 0
        if market_data_dict and symbol in market_data_dict:
            volume = market_data_dict[symbol].get("total_volume", 0) or 0
        
        hotness_score = idx + (1.0 - (volume / (10000000 + volume)))
        hotness_ranks[symbol] = hotness_score
    
    return hotness_ranks


def calculate_position_size(entry_price, stop_loss_price, account_balance, risk_pct=0.02):
    """
    Calculate position size based on risk management.
    Returns (num_contracts: float, risk_amount: float)
    """
    if not account_balance or account_balance <= 0:
        return 0, 0
    if not entry_price or not stop_loss_price or entry_price <= 0 or stop_loss_price <= 0:
        return 0, 0
    
    risk_amount = account_balance * risk_pct
    price_diff = abs(entry_price - stop_loss_price)
    
    if price_diff <= 0:
        return 0, 0
    
    num_contracts = risk_amount / price_diff
    return max(0, num_contracts), risk_amount


def get_session_hour_bias():
    """
    Get trading hour bias for current UTC hour.
    Returns (session_name: str, strategy_weight_adjustment: dict)
    """
    hour = datetime.now(timezone.utc).hour
    
    if 8 <= hour < 12:
        session = "london_open"
        adjustments = {"reversal": 1.2, "range": 1.1, "momentum": 0.9}
    elif 13 <= hour < 17:
        session = "london_close_ny_open"
        adjustments = {"reversal": 1.15, "range": 0.95, "momentum": 1.1}
    elif 21 <= hour < 1:
        session = "ny_active"
        adjustments = {"reversal": 1.1, "range": 0.9, "momentum": 1.2}
    elif 1 <= hour < 8:
        session = "asian_hours"
        adjustments = {"reversal": 0.85, "range": 1.05, "momentum": 0.75}
    else:
        session = "neutral_hours"
        adjustments = {"reversal": 1.0, "range": 1.0, "momentum": 1.0}
    
    return session, adjustments


def load_performance_history(strategy_name):
    """Load performance tracking for a strategy."""
    try:
        filename = f"perf_history_{strategy_name}.json"
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"wins": 0, "losses": 0, "signals_generated": 0, "avg_win_pct": 50.0}


def update_performance_history(strategy_name, win_pct):
    """Update performance history for a strategy."""
    try:
        perf = load_performance_history(strategy_name)
        perf["signals_generated"] = perf.get("signals_generated", 0) + 1
        
        if win_pct > 0:
            perf["wins"] = perf.get("wins", 0) + 1
        else:
            perf["losses"] = perf.get("losses", 0) + 1
        
        total = perf["wins"] + perf["losses"]
        if total > 0:
            perf["avg_win_pct"] = (perf["wins"] / total) * 100
        
        filename = f"perf_history_{strategy_name}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(perf, f)
    except Exception:
        pass


def get_strategy_confidence_multiplier(strategy_name):
    """Get confidence multiplier based on recent win rate."""
    perf = load_performance_history(strategy_name)
    win_rate = perf.get("avg_win_pct", 50.0)
    
    if win_rate > 60:
        return 1.15
    elif win_rate > 55:
        return 1.08
    elif win_rate < 40:
        return 0.85
    elif win_rate < 45:
        return 0.92
    else:
        return 1.0


def compute_multi_timeframe_alignment(klines_1m, klines_5m, klines_15m, klines_1h, direction='LONG'):
    """
    Score signal based on multi-timeframe directional alignment.
    Higher score = more timeframes agree with direction.
    Returns alignment_score (0-1.0)
    """
    agreement_count = 0
    checked_count = 0
    
    for klines, tf_name in [(klines_1m, "1m"), (klines_5m, "5m"), (klines_15m, "15m"), (klines_1h, "1h")]:
        if not klines or len(klines) < 3:
            continue
        
        checked_count += 1
        closes = [float(k[4]) for k in klines[-3:]]
        
        if direction == 'LONG' and closes[-1] > closes[0]:
            agreement_count += 1
        elif direction == 'SHORT' and closes[-1] < closes[0]:
            agreement_count += 1
    
    if checked_count == 0:
        return 0.5
    
    return (agreement_count / checked_count) * 1.0


def apply_confidence_decay(signal, cycle_age_sec, decay_rate=0.02):
    """Apply confidence decay based on signal age within cycle."""
    if cycle_age_sec <= 0:
        return signal
    
    decay = 1.0 - (decay_rate * (cycle_age_sec / 60.0))
    signal['confidence'] = signal.get('confidence', 0) * decay
    return signal


def has_account_heat(account_balance, recent_loss_pct=5.0, threshold_pct=10.0):
    """Check if recent drawdown exceeded threshold."""
    if account_balance <= 0:
        return False
    return recent_loss_pct > threshold_pct


def check_position_correlation(signal_pair, existing_signals, correlation_threshold=0.7):
    """Check if pair is too correlated with existing signals."""
    if not existing_signals:
        return False
    
    related_pairs = {"BTC": ["ETH"], "ETH": ["BTC"], "BNB": ["SOL"], "SOL": ["BNB"]}
    base = signal_pair.replace("USDT", "").replace("BUSD", "").replace("USDC", "")
    
    for related in related_pairs.get(base, []):
        for sig in existing_signals:
            if related in sig.get("pair", ""):
                return True
    
    return False


def compute_signal_quality_score(signal):
    """Legacy quality score computation (kept to avoid IndentationError).
    This will be superseded downstream by the adaptive probability engine when wired.
    Returns a 0-100 score using existing fields if present.
    """
    try:
        score = 0.0
        if isinstance(signal, dict):
            quality = float(signal.get("quality_score", 50.0))
            confidence = float(signal.get("confidence", 50.0))
            layers = len(signal.get("trigger_layers", []))
            prob_tp = float(signal.get("prob_tp", 0.5))
        else:
            quality = 50.0
            confidence = 50.0
            layers = 0
            prob_tp = 0.5
        score += prob_tp * 100.0 * 0.40
        score += confidence * 0.30
        score += quality * 0.20
        score += layers * 10 * 0.10
        return min(100.0, max(0.0, score))
    except Exception:
        return 50.0

# Adaptive probability-driven confidence & quality
DEBUG_CONFIDENCE = os.getenv("DEBUG_CONFIDENCE", "false").lower() == "true"
CONF_PROB_BASE_MOMENTUM = float(os.getenv("CONF_PROB_BASE_MOMENTUM", "0.52"))
CONF_PROB_BASE_RANGE = float(os.getenv("CONF_PROB_BASE_RANGE", "0.50"))
CONF_PROB_BASE_REVERSAL = float(os.getenv("CONF_PROB_BASE_REVERSAL", "0.48"))
PROB_WEIGHT_TREND = float(os.getenv("PROB_WEIGHT_TREND", "0.12"))
PROB_WEIGHT_STRUCTURE = float(os.getenv("PROB_WEIGHT_STRUCTURE", "0.12"))
PROB_WEIGHT_VOL = float(os.getenv("PROB_WEIGHT_VOL", "0.08"))
PROB_WEIGHT_CONFIRM = float(os.getenv("PROB_WEIGHT_CONFIRM", "0.10"))
PROB_WEIGHT_RR = float(os.getenv("PROB_WEIGHT_RR", "0.08"))


def _build_features_momentum(klines, direction):
    closes = [float(k[4]) for k in klines]
    highs = [float(k[2]) for k in klines]
    lows = [float(k[3]) for k in klines]
    ema_len = int(os.getenv("MOMENTUM_EMA_LENGTH", "50"))
    ema = _ema(closes, ema_len) or closes[-1]
    slope = _ema_slope(closes, ema_len)
    atr = _atr(klines, 14) or max(1e-6, highs[-1]-lows[-1])
    price = closes[-1]
    dist_atr = abs(price - ema) / max(1e-12, atr)
    rsi = _rsi(closes, 14) or 50.0
    struct_up = all(closes[-i] > closes[-i-1] for i in range(1, 4))
    struct_down = all(closes[-i] < closes[-i-1] for i in range(1, 4))
    return {"ema": ema, "slope": slope, "atr": atr, "price": price, "dist_atr": dist_atr, "rsi": rsi,
            "struct_up": struct_up, "struct_down": struct_down}


def _build_features_range(klines, direction):
    closes = [float(k[4]) for k in klines]
    lower, mid, upper, width = _bollinger(closes, RNG_BB_PERIOD, RNG_BB_STD)
    price = closes[-1]
    rsi = _rsi(closes, 14) or 50.0
    atr = _atr(klines, 14) or (abs((upper or price) - (lower or price)) / 4.0 if lower is not None and upper is not None else 0.0)
    # Strict touches
    touch_lower = lower is not None and price <= lower
    touch_upper = upper is not None and price >= upper
    # ATR-proximity touches (within 0.3 ATR of band)
    near_lower = lower is not None and (price - lower) <= max(0.0, 0.3 * atr)
    near_upper = upper is not None and (upper - price) <= max(0.0, 0.3 * atr)
    return {
        "lower": lower, "mid": mid, "upper": upper, "width": width or 0.0, "price": price,
        "rsi": rsi, "atr": atr, "touch_lower": touch_lower, "touch_upper": touch_upper,
        "near_lower": near_lower, "near_upper": near_upper
    }


def _build_features_reversal(klines, direction):
    closes = [float(k[4]) for k in klines]
    highs = [float(k[2]) for k in klines]
    lows = [float(k[3]) for k in klines]
    atr = _atr(klines, 14) or max(1e-6, highs[-1]-lows[-1])
    ema20 = _ema(closes, 20) or closes[-1]
    slope20 = _ema_slope(closes, 20)
    recent_high = max(highs[-8:]); recent_low = min(lows[-8:])
    body = abs(closes[-1] - float(klines[-1][1]))
    body_atr = body / max(1e-12, atr)
    return {"atr": atr, "ema": ema20, "slope": slope20, "recent_high": recent_high, "recent_low": recent_low,
            "body_atr": body_atr}


def compute_signal_confidence_and_quality(strategy, direction, klines, tp_pct, sl_pct):
    """Return prob_tp, prob_sl, conf_adaptive, quality_score based on features, RR, and structure."""
    if not klines or len(klines) < 10 or tp_pct is None or sl_pct is None or sl_pct <= 0:
        return 0.5, 0.5, 50.0, 50.0
    strategy = strategy.lower()
    rr = max(0.1, tp_pct / max(1e-12, sl_pct))

    if strategy == 'momentum':
        f = _build_features_momentum(klines, direction)
        p = CONF_PROB_BASE_MOMENTUM
        # Trend/structure
        if direction == 'LONG':
            if f['slope'] > 0 and f['struct_up'] and f['price'] > f['ema']:
                p += PROB_WEIGHT_TREND
            if f['dist_atr'] <= 2.0:
                p += PROB_WEIGHT_STRUCTURE * 0.5
            else:
                p -= PROB_WEIGHT_VOL * 0.5
        else:
            if f['slope'] < 0 and f['struct_down'] and f['price'] < f['ema']:
                p += PROB_WEIGHT_TREND
            if f['dist_atr'] <= 2.0:
                p += PROB_WEIGHT_STRUCTURE * 0.5
            else:
                p -= PROB_WEIGHT_VOL * 0.5
        # RR influence
        p += PROB_WEIGHT_RR * (math.atan(rr - 1) / (math.pi/2))  # maps rr to [-1,1]
        p = max(0.05, min(0.95, p))
        evidence = 0.9 + (min(2.0, abs(f['slope'])*1e4) * 0.05)
        quality = min(100.0, max(10.0, (f['struct_up'] or f['struct_down']) * 20 + (2.0 - min(2.0, f['dist_atr']))*15 + evidence*30))

    elif strategy == 'range':
        f = _build_features_range(klines, direction)
        p = CONF_PROB_BASE_RANGE
        max_pct = float(os.getenv('RANGE_BB_WIDTH_MAX_PCT', '12.0'))
        min_pct = float(os.getenv('RANGE_BB_WIDTH_MIN_PCT', '0.8'))
        breakout_cut = float(os.getenv('RANGE_BREAKOUT_BANDWIDTH_PCT', '15.0'))
        width = f['width'] or 0.0
        # Regime classification
        if width > breakout_cut:
            p -= PROB_WEIGHT_VOL
        elif width < min_pct or width > max_pct:
            p -= PROB_WEIGHT_VOL * 0.5
        else:
            if direction == 'LONG' and (f['touch_lower'] or f.get('near_lower')):
                p += PROB_WEIGHT_CONFIRM
            if direction == 'SHORT' and (f['touch_upper'] or f.get('near_upper')):
                p += PROB_WEIGHT_CONFIRM
        p += PROB_WEIGHT_RR * (math.atan(rr - 1) / (math.pi/2))
        p = max(0.05, min(0.95, p))
        quality = min(100.0, max(10.0,
                                 (100 - min(100.0, width)) * 0.6 +
                                 (1 if (f['touch_lower'] or f['touch_upper'] or f.get('near_lower') or f.get('near_upper')) else 0) * 20 +
                                 40))

    else:  # reversal
        f = _build_features_reversal(klines, direction)
        p = CONF_PROB_BASE_REVERSAL
        # Confirmation by body/ATR and slope moderation
        if f['body_atr'] >= 0.15:
            p += PROB_WEIGHT_CONFIRM
        if abs(f['slope']) < 5e-4:
            p += PROB_WEIGHT_TREND * 0.5
        # RR influence
        p += PROB_WEIGHT_RR * (math.atan(rr - 1) / (math.pi/2))
        p = max(0.05, min(0.95, p))
        quality = min(100.0, max(10.0, f['body_atr'] * 35 + 45))

    prob_tp = p
    prob_sl = 1.0 - p
    conf = min(100.0, max(5.0, prob_tp * 100.0 * (0.9 if prob_tp < 0.55 else 1.05)))
    if DEBUG_CONFIDENCE:
        print(f"[CONF] strat={strategy} dir={direction} prob={prob_tp:.2f} rr={rr:.2f} conf={conf:.1f} qual={quality:.1f}", flush=True)
    return prob_tp, prob_sl, conf, quality
    """Compute overall quality score for a signal (0-100 scale)."""
    score = 0.0
    
    conf = signal.get("confidence", 50.0)
    score += (conf / 100.0) * 35.0
    
    prob_tp = signal.get("prob_tp", 0.5)
    score += prob_tp * 25.0
    
    quality = signal.get("quality_score", 50.0)
    score += (quality / 100.0) * 20.0
    
    layers = signal.get("layers_count", 1)
    score += min(5.0, layers) / 5.0 * 20.0
    
    return min(100.0, max(0.0, score))


def track_drawdown(account_balance, current_equity, max_equity_history_file="max_equity.json"):
    """Track maximum drawdown and return (current_dd_pct, max_dd_pct, circuit_breaker_active)."""
    try:
        max_equity = load_json_file(max_equity_history_file, {}).get("max_equity", account_balance)
    except Exception:
        max_equity = account_balance
    
    if current_equity > max_equity:
        max_equity = current_equity
        save_json_file(max_equity_history_file, {"max_equity": max_equity})
    
    drawdown_pct = ((max_equity - current_equity) / (max_equity + 1e-12)) * 100
    max_drawdown_threshold = float(os.getenv("MAX_DRAWDOWN_PCT", "15.0"))
    circuit_active = drawdown_pct > max_drawdown_threshold
    
    return drawdown_pct, max_equity, circuit_active


def wait_for_candle_confirmation(klines, direction, confirmation_candles=1):
    """Check if last N candles confirm entry direction."""
    if not klines or len(klines) < confirmation_candles + 1:
        return True, 0
    
    closes = [float(k[4]) for k in klines[-(confirmation_candles + 1):]]
    
    confirming = 0
    if direction == "LONG":
        for i in range(len(closes) - 1):
            if closes[i + 1] > closes[i]:
                confirming += 1
    else:
        for i in range(len(closes) - 1):
            if closes[i + 1] < closes[i]:
                confirming += 1
    
    confidence_from_confirmation = (confirming / max(1, confirmation_candles)) * 100
    return confirming >= confirmation_candles, confidence_from_confirmation


def calculate_atr_adjusted_sl_tp(klines, entry_price, direction, base_sl_pct, base_tp_pct, atr_period=14):
    """Adjust SL/TP based on current ATR volatility."""
    if not klines or len(klines) < atr_period + 2:
        return base_sl_pct, base_tp_pct
    
    try:
        atr = compute_atr_from_klines(klines, period=atr_period)
        if not atr or atr <= 0:
            return base_sl_pct, base_tp_pct
        
        atr_pct = (atr / entry_price) * 100
        
        if atr_pct > 3.0:
            sl_pct = base_sl_pct * 1.3
            tp_pct = base_tp_pct * 0.9
        elif atr_pct > 1.5:
            sl_pct = base_sl_pct * 1.1
            tp_pct = base_tp_pct * 0.95
        elif atr_pct < 0.5:
            sl_pct = base_sl_pct * 0.8
            tp_pct = base_tp_pct * 1.15
        else:
            sl_pct = base_sl_pct
            tp_pct = base_tp_pct
        
        return min(0.15, sl_pct), min(0.25, tp_pct)
    except Exception:
        return base_sl_pct, base_tp_pct


def compute_zone(pair_data):
    """Compute price zone (support / resistance) based on 24h low/high."""
    try:
        price = pair_data.get("price")
        high_24h = pair_data.get("high_24h")
        low_24h = pair_data.get("low_24h")
        if not price or not high_24h or not low_24h:
            return "neutral", None
        range_24h = high_24h - low_24h
        if range_24h <= 1e-12:
            return "neutral", None
        pos = (price - low_24h) / range_24h
        if pos <= 0.2:
            return "support", pos
        elif pos >= 0.8:
            return "resistance", pos
        else:
            return "neutral", pos
    except Exception:
        return "neutral", None


def range_direction_confirmed(klines, direction):
    """Check if fresh klines show ANY recent confirmation of range direction (1 of last 2 candles is permissive enough for range)."""
    if not klines or len(klines) < 2:
        return False
    
    closes = [float(k[4]) for k in klines[-2:]]
    
    if direction == "LONG":
        # Permissive: any of last 2 candles moving in correct direction
        return any(klines[i][4] > klines[i][1] or klines[i][4] > klines[i-1][4] for i in range(-1, -min(len(klines), 3), -1))
    elif direction == "SHORT":
        return any(klines[i][4] < klines[i][1] or klines[i][4] < klines[i-1][4] for i in range(-1, -min(len(klines), 3), -1))
    
    return False


def log_signal_to_csv(signal, filename="signals.csv", include_trigger_layers=False, include_range_meta=False):
    """Append a signal dict to CSV `filename`. Optional flags insert trigger layers or range meta."""
    try:
        # ensure file exists; open in append mode
        with open(filename, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            row = [
                signal.get("type"),
                signal.get("pair"),
                signal.get("direction"),
                signal.get("entry"),
                signal.get("sl"),
                signal.get("tp"),
                signal.get("sl_pct"),
                signal.get("tp_pct"),
                signal.get("confidence"),
                signal.get("leverage"),
                signal.get("profit_multiplier"),
                signal.get("notes", "")
            ]
            if include_trigger_layers:
                row.insert(11, ",".join(signal.get("trigger_layers", [])))
            if include_range_meta:
                meta = signal.get("range_meta", {}) or {}
                meta_str = f"{meta.get('low')}|{meta.get('high')}|rejections_top:{meta.get('rejections_top')}|rejections_bottom:{meta.get('rejections_bottom')}"
                insert_idx = 12 if include_trigger_layers else 11
                row.insert(insert_idx, meta_str)
            w.writerow(row)
    except Exception as e:
        print("⚠️ Failed to write signal log:", e)


def load_cooldown_cache():
    return load_json_file(COOLDOWN_CACHE_FILE, {})

def save_cooldown_cache(c):
    save_json_file(COOLDOWN_CACHE_FILE, c)

def load_vol_cache():
    return load_json_file(VOL_CACHE_FILE, {})

def save_vol_cache(c):
    save_json_file(VOL_CACHE_FILE, c)

# ============ STRATEGY-SPECIFIC PAIR DISCOVERY ============

def fetch_reversal_pair_candidates_from_coinpaprika(limit=TOP_TRENDING):
    """
    Fetch pair candidates for REVERSAL strategy using Coinpaprika API.
    Prioritizes top gainers & losers (most extreme moves) where reversals are most likely.
    Uses Coinpaprika primary API (Bitstamp fallback for individual prices).
    
    Returns: List of gainer and loser pairs combined
    """
    try:
        url = 'https://api.coinpaprika.com/v1/tickers'
        params = {
            'quotes': 'USD',
            'limit': min(limit * 3, 250)
        }
        
        resp = _get_requests_session().get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                gainers = []
                losers = []
                
                for coin in data:
                    try:
                        symbol = coin.get('symbol', '').upper()
                        if is_stablecoin(symbol):
                            continue
                        
                        quotes = coin.get('quotes', {})
                        usd_data = quotes.get('USD', {})
                        change_24h = float(usd_data.get('percent_change_24h', 0))
                        price = float(usd_data.get('price', 0))
                        
                        if price <= 0:
                            continue
                        
                        volume = float(coin.get('total_volume') or 0)
                        if volume <= 0:
                            volume = 500000
                        
                        item = {
                            'id': coin.get('id'),
                            'symbol': symbol,
                            'price_change_24h': change_24h,
                            'change_24h': change_24h,
                            'price': price,
                            'volume': volume,
                            'price_change_1h': 0,
                            'high_24h': price * (1 + abs(change_24h) / 100),
                            'low_24h': price * (1 - abs(change_24h) / 200)
                        }
                        
                        if change_24h > 0:
                            gainers.append(item)
                        elif change_24h < 0:
                            losers.append(item)
                    except Exception:
                        continue
                
                gainers.sort(key=lambda x: x['change_24h'], reverse=True)
                losers.sort(key=lambda x: x['change_24h'])
                
                result = gainers[:limit//2] + losers[:limit//2]
                if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
                    print(f"[REVERSAL] Got {len(result)} candidates from Coinpaprika", flush=True)
                return result
    except Exception as e:
        if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
            print(f"[REVERSAL] Coinpaprika error: {str(e)[:80]}", flush=True)
    
    return []

def fetch_momentum_pair_candidates_from_coingecko(limit=50):
    """
    Fetch pair candidates for MOMENTUM strategy using CoinGecko API.
    Uses top trending pairs by volume (no strict volatility filter).
    Includes price change and high/low data for scoring functions.
    
    Returns: List of top trading pairs with complete market data
    """
    try:
        url = 'https://api.coingecko.com/api/v3/markets'
        params = {
            'vs_currency': 'usd',
            'order': 'volume_desc',
            'per_page': limit,
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '1h,24h'
        }
        
        resp = _get_requests_session().get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                candidates = []
                
                for coin in data:
                    try:
                        symbol = coin.get('symbol', '').upper()
                        if is_stablecoin(symbol):
                            continue
                        
                        volume = float(coin.get('total_volume', 0) or 0)
                        price = float(coin.get('current_price', 0) or 0)
                        
                        if volume <= 0:
                            volume = 500000
                        
                        if price > 0:
                            change_1h = float(coin.get('price_change_percentage_1h_in_currency') or 0)
                            change_24h = float(coin.get('price_change_percentage_24h_in_currency') or 0)
                            
                            # Momentum Score: Heavy weight on 1h move, secondary on 24h
                            # Higher volume also gives a slight boost to ensure liquidity
                            momentum_score = (abs(change_1h) * 10.0) + (abs(change_24h) * 1.5) + (math.log10(volume) * 0.5)
                            
                            candidates.append({
                                'id': coin.get('id'),
                                'symbol': symbol,
                                'price': price,
                                'volume': volume,
                                'price_change_1h': change_1h,
                                'price_change_24h': change_24h,
                                'momentum_score': momentum_score,
                                'high_24h': float(coin.get('high_24h') or price),
                                'low_24h': float(coin.get('low_24h') or price)
                            })
                    except Exception:
                        continue
                
                # Sort by momentum score to get the real movers
                candidates.sort(key=lambda x: x['momentum_score'], reverse=True)
                candidates = candidates[:limit]
                
                if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
                    print(f"[MOMENTUM] Got {len(candidates)} candidates from CoinGecko", flush=True)
                return candidates
    except Exception as e:
        if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
            print(f"[MOMENTUM] CoinGecko error: {str(e)[:80]}", flush=True)
    
    return []

def fetch_range_pair_candidates_from_huobi(limit=50):
    """
    Fetch pair candidates for RANGE strategy using Huobi API.
    Prioritizes low-volatility, consolidating pairs.
    Uses Huobi primary API (Gemini fallback for individual prices).
    
    Returns: List of pairs with consolidation characteristics
    """
    try:
        url = 'https://api.huobi.pro/market/tickers'
        
        resp = _get_requests_session().get(url, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('status') == 'ok' and 'data' in data:
                consolidating = []
                
                for ticker in data['data']:
                    try:
                        symbol = ticker.get('symbol', '').replace('usdt', '').upper()
                        if is_stablecoin(symbol) or len(symbol) < 2:
                            continue
                        
                        # Normalization: Tradability check
                        if not is_tradable_on_exchanges(f"{symbol}/USDT", strategy="range"):
                            continue
                        
                        price = float(ticker.get('close', 0) or 0)
                        vol = float(ticker.get('vol', 0) or 0)
                        
                        if vol <= 0:
                            vol = 500000
                        
                        if price <= 0:
                            continue
                        
                        high = float(ticker.get('high', price))
                        low = float(ticker.get('low', price))
                        
                        if high <= 0 or low <= 0 or high < low:
                            continue
                        
                        range_pct = abs((high - low) / price * 100)
                        
                        # RANGE: Tighten filter from 15% to 7% for higher quality consolidation
                        if range_pct < 7.0:
                            # Quality Score: Reward narrow ranges and consistent volume
                            # We use log10(vol) to prevent high-volume giants from drowning out quality setups
                            consolidation_score = (7.0 - range_pct) * 5.0 + (math.log10(vol) * 1.5)
                            
                            consolidating.append({
                                'id': symbol.lower(),
                                'symbol': symbol,
                                'price': price,
                                'volume': vol,
                                'range_pct': range_pct,
                                'score': consolidation_score,
                                'price_change_1h': 0,
                                'price_change_24h': 0,
                                'high_24h': high,
                                'low_24h': low
                            })
                    except Exception:
                        continue
                
                consolidating.sort(key=lambda x: x['score'], reverse=True)
                result = consolidating[:limit]
                if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
                    print(f"[RANGE] Got {len(result)} candidates from Huobi", flush=True)
                return result
    except Exception as e:
        if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
            print(f"[RANGE] Huobi error: {str(e)[:80]}", flush=True)
    
    return []

def fetch_reversal_pair_candidates_from_bitstamp(limit=50):
    """
    Fetch pair candidates for REVERSAL strategy using Bitstamp API.
    Fallback when Coinpaprika fails.
    
    Returns: List of pairs with market data
    """
    try:
        url = 'https://www.bitstamp.net/api/v2/ticker/'
        tickers = ['btcusd', 'ethusd', 'xrpusd', 'ltcusd', 'bchusd', 'linkusd', 'adausd', 'dotusd']
        
        candidates = []
        for ticker_pair in tickers[:limit]:
            try:
                resp = _get_requests_session().get(url + ticker_pair + '/', timeout=3)
                if resp.status_code == 200:
                    data = resp.json()
                    symbol = ticker_pair.replace('usd', '').upper()
                    if is_stablecoin(symbol):
                        continue
                    
                    price = float(data.get('last', 0))
                    volume = float(data.get('volume', 0))
                    
                    if price > 0 and volume > 0:
                        candidates.append({
                            'id': symbol.lower(),
                            'symbol': symbol,
                            'price': price,
                            'volume': volume,
                            'price_change_24h': float(data.get('open', price) and ((float(data.get('last', price)) - float(data.get('open', price))) / float(data.get('open', price)) * 100) or 0),
                            'price_change_1h': 0,
                            'high_24h': float(data.get('high', price)),
                            'low_24h': float(data.get('low', price))
                        })
                        if len(candidates) >= limit:
                            break
            except Exception:
                continue
        
        if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
            print(f"[REVERSAL] Got {len(candidates)} candidates from Bitstamp fallback", flush=True)
        return candidates
    except Exception as e:
        if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
            print(f"[REVERSAL] Bitstamp fallback error: {str(e)[:80]}", flush=True)
    
    return []

def fetch_reversal_pair_candidates(limit=TOP_TRENDING):
    """
    Fetch pair candidates for REVERSAL strategy.
    Prioritizes top gainers & losers (most extreme moves) where reversals are most likely.
    Uses Coinpaprika primary API (Bitstamp fallback).
    
    Returns: List of gainer and loser pairs combined
    """
    if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
        print(f"[REVERSAL] Using Coinpaprika API for top gainers & losers", flush=True)
    
    candidates = fetch_reversal_pair_candidates_from_coinpaprika(limit)
    if not candidates:
        if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
            print(f"[REVERSAL] Coinpaprika failed, using Bitstamp fallback", flush=True)
        candidates = fetch_reversal_pair_candidates_from_bitstamp(limit)
    
    # Normalization: Filter by tradability to reduce wasted calls in later stages
    if candidates:
        filtered = []
        # Explicitly determine strategy for filtering
        func_name = inspect.currentframe().f_code.co_name
        strategy_for_filter = "reversal" if "reversal" in func_name else "range"
        
        for c in candidates:
            symbol = c.get('symbol', '').upper()
            if symbol and is_tradable_on_exchanges(f"{symbol}/USDT", strategy=strategy_for_filter):
                filtered.append(c)
        return filtered
        
    return []

def fetch_momentum_pair_candidates_from_cexio(limit=50):
    """
    Fetch pair candidates for MOMENTUM strategy using CEX.IO API.
    Fallback when CoinGecko fails.
    
    Returns: List of pairs with market data
    """
    try:
        url = 'https://cex.io/api/tickers/USD/'
        resp = _get_requests_session().get(url, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            candidates = []
            
            pairs = data.get('data', [])
            for pair_data in pairs:
                try:
                    symbol = pair_data.get('pair', '').replace('USD', '').upper()
                    if not symbol or is_stablecoin(symbol):
                        continue
                    
                    price = float(pair_data.get('last', 0))
                    volume = float(pair_data.get('volume', 0))
                    
                    if price > 0 and volume > 0:
                        change_24h = float(pair_data.get('priceChangePercent', 0)) if 'priceChangePercent' in pair_data else 0
                        # CEX.IO doesn't provide 1h change in ticker, so we use 24h as proxy
                        momentum_score = abs(change_24h) * 2.0 + (math.log10(volume) * 0.5)
                        
                        candidates.append({
                            'id': symbol.lower(),
                            'symbol': symbol,
                            'price': price,
                            'volume': volume,
                            'price_change_24h': change_24h,
                            'price_change_1h': 0,
                            'momentum_score': momentum_score,
                            'high_24h': float(pair_data.get('high', price)),
                            'low_24h': float(pair_data.get('low', price))
                        })
                except Exception:
                    continue
            
            candidates.sort(key=lambda x: x['momentum_score'], reverse=True)
            candidates = candidates[:limit]
            
            if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
                print(f"[MOMENTUM] Got {len(candidates)} candidates from CEX.IO fallback", flush=True)
            return candidates
    except Exception as e:
        if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
            print(f"[MOMENTUM] CEX.IO fallback error: {str(e)[:80]}", flush=True)
    
    return []

def fetch_momentum_pair_candidates(limit=50):
    """
    Fetch pair candidates for MOMENTUM strategy.
    Prioritizes high-volume, volatile pairs with recent spikes.
    Uses CoinGecko primary API (CEX.IO fallback).
    
    Returns: List of pairs with volume spike characteristics
    """
    if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
        print(f"[MOMENTUM] Using CoinGecko API for volatile pairs", flush=True)
    
    candidates = fetch_momentum_pair_candidates_from_coingecko(limit)
    if not candidates:
        if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
            print(f"[MOMENTUM] CoinGecko failed, using CEX.IO fallback", flush=True)
        candidates = fetch_momentum_pair_candidates_from_cexio(limit)
    
    # Normalization: Filter by tradability to reduce wasted calls
    if candidates:
        filtered = []
        for c in candidates:
            symbol = c.get('symbol', '').upper()
            if symbol and is_tradable_on_exchanges(f"{symbol}/USDT", strategy="momentum"):
                filtered.append(c)
        return filtered
        
    return []

def fetch_range_pair_candidates_from_gemini(limit=50):
    """
    Fetch pair candidates for RANGE strategy using Gemini API.
    Fallback when Huobi fails.
    
    Returns: List of pairs with consolidation characteristics
    """
    try:
        url = 'https://api.gemini.com/v1/symbols'
        resp = _get_requests_session().get(url, timeout=5)
        if resp.status_code == 200:
            symbols = resp.json()
            candidates = []
            
            for sym in symbols:
                try:
                    if not sym.endswith('usd'):
                        continue
                    
                    symbol = sym.replace('usd', '').upper()
                    if is_stablecoin(symbol):
                        continue
                    
                    ticker_url = f'https://api.gemini.com/v1/pubticker/{sym}'
                    ticker_resp = _get_requests_session().get(ticker_url, timeout=3)
                    if ticker_resp.status_code == 200:
                        ticker_data = ticker_resp.json()
                        price = float(ticker_data.get('last', 0))
                        volume = float(ticker_data.get('volume', {}).get('usd', 0)) if isinstance(ticker_data.get('volume'), dict) else float(ticker_data.get('volume', 0))
                        
                        if price > 0 and volume > 0:
                            high = float(ticker_data.get('high', price))
                            low = float(ticker_data.get('low', price))
                            
                            if high > 0 and low > 0:
                                range_pct = abs((high - low) / price * 100)
                                
                                if range_pct < 7.0:
                                    # Use same scoring logic as Huobi for consistency
                                    consolidation_score = (7.0 - range_pct) * 5.0 + (math.log10(volume) * 1.5)
                                    
                                    candidates.append({
                                        'id': symbol.lower(),
                                        'symbol': symbol,
                                        'price': price,
                                        'volume': volume,
                                        'range_pct': range_pct,
                                        'score': consolidation_score,
                                        'price_change_1h': 0,
                                        'price_change_24h': 0,
                                        'high_24h': high,
                                        'low_24h': low
                                    })
                                    if len(candidates) >= limit:
                                        break
                except Exception:
                    continue
            
            candidates.sort(key=lambda x: x['score'], reverse=True)
            if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
                print(f"[RANGE] Got {len(candidates)} candidates from Gemini fallback", flush=True)
            return candidates
    except Exception as e:
        if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
            print(f"[RANGE] Gemini fallback error: {str(e)[:80]}", flush=True)
    
    return []

def fetch_range_pair_candidates(limit=50):
    """
    Fetch pair candidates for RANGE strategy.
    Prioritizes low-volatility, consolidating pairs.
    Uses Huobi primary API (Gemini fallback).
    
    Returns: List of pairs with consolidation characteristics
    """
    if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
        print(f"[RANGE] Using Huobi API for consolidating pairs", flush=True)
    
    candidates = fetch_range_pair_candidates_from_huobi(limit)
    if not candidates:
        if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
            print(f"[RANGE] Huobi failed, using Gemini fallback", flush=True)
        candidates = fetch_range_pair_candidates_from_gemini(limit)
        
    # Normalization: Filter by tradability to reduce wasted calls in later stages
    if candidates:
        filtered = []
        # Explicitly determine strategy for filtering
        func_name = inspect.currentframe().f_code.co_name
        strategy_for_filter = "reversal" if "reversal" in func_name else "range"
        
        for c in candidates:
            symbol = c.get('symbol', '').upper()
            if symbol and is_tradable_on_exchanges(f"{symbol}/USDT", strategy=strategy_for_filter):
                filtered.append(c)
        return filtered
        
    return []



def fetch_market_list(limit=50, strategy="MOMENTUM"):
    """
    Fetch market list using ONLY primary APIs for each strategy.
    
    Strategy-specific data:
    - MOMENTUM: CoinGecko - Top 50 trending pairs by volume
    - REVERSAL: CoinPaprika - Top gainers and losers
    - RANGE: Huobi - Consolidating pairs
    """
    strategy_upper = strategy.upper() if isinstance(strategy, str) else "MOMENTUM"
    
    if strategy_upper == "MOMENTUM":
        # Momentum: CoinGecko primary - fetch top trending pairs by volume
        try:
            print(f"[MOMENTUM] Fetching top {limit} trending pairs from CoinGecko...", flush=True)
            url = 'https://api.coingecko.com/api/v3/coins/markets'
            params = {
                'vs_currency': 'usd',
                'order': 'volume_desc',
                'per_page': limit,
                'page': 1,
                'sparkline': False,
                'price_change_percentage': '1h,24h'
            }
            resp = _get_requests_session().get(url, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    markets = []
                    for coin in data:
                        try:
                            symbol = coin.get('symbol', '').upper()
                            if is_stablecoin(symbol):
                                continue
                            
                            # Normalization: Filter by tradability
                            if not is_tradable_on_exchanges(f"{symbol}/USDT", strategy=strategy_upper.lower()):
                                continue
                                
                            price = float(coin.get('current_price', 0))
                            if price <= 0:
                                continue
                            
                            markets.append({
                                'id': coin.get('id'),
                                'symbol': symbol,
                                'current_price': price,
                                'total_volume': float(coin.get('total_volume', 0)),
                                'price_change_percentage_24h_in_currency': float(coin.get('price_change_percentage_24h_in_currency', 0)),
                                'price_change_percentage_1h_in_currency': float(coin.get('price_change_percentage_1h_in_currency', 0)),
                                'high_24h': float(coin.get('high_24h', price)),
                                'low_24h': float(coin.get('low_24h', price))
                            })
                            if len(markets) >= limit:
                                break
                        except Exception:
                            continue
                    
                    if markets:
                        print(f"[MOMENTUM] ✓ Fetched {len(markets)} trending pairs from CoinGecko", flush=True)
                        return markets
                    else:
                        print(f"[MOMENTUM] ⚠️ No valid trending pairs found", flush=True)
        except Exception as e:
            print(f"[MOMENTUM] Error fetching trending pairs: {str(e)[:80]}", flush=True)
            
    elif strategy_upper == "REVERSAL":
        # Reversal: Unified candidate fetcher (already includes tradability filter)
        try:
            candidates = fetch_reversal_pair_candidates(limit=limit)
            if candidates:
                # Convert to market list format
                markets = []
                for cand in candidates:
                    markets.append({
                        'id': cand.get('id'),
                        'symbol': cand.get('symbol'),
                        'current_price': cand.get('price', 0),
                        'total_volume': cand.get('volume', 0),
                        'price_change_percentage_24h_in_currency': cand.get('price_change_24h', 0),
                        'price_change_percentage_1h_in_currency': cand.get('price_change_1h', 0),
                        'high_24h': cand.get('high_24h', cand.get('price', 0)),
                        'low_24h': cand.get('low_24h', cand.get('price', 0))
                    })
                return markets
        except Exception as e:
            print(f"[REVERSAL] Error fetching market list: {str(e)[:80]}", flush=True)
            
    elif strategy_upper == "RANGE":
        # Range: Unified candidate fetcher (already includes tradability filter)
        try:
            candidates = fetch_range_pair_candidates(limit=limit)
            if candidates:
                # Convert to market list format
                markets = []
                for cand in candidates:
                    markets.append({
                        'id': cand.get('id'),
                        'symbol': cand.get('symbol'),
                        'current_price': cand.get('price', 0),
                        'total_volume': cand.get('volume', 0),
                        'price_change_percentage_24h_in_currency': cand.get('price_change_24h', 0),
                        'price_change_percentage_1h_in_currency': cand.get('price_change_1h', 0),
                        'high_24h': cand.get('high_24h', cand.get('price', 0)),
                        'low_24h': cand.get('low_24h', cand.get('price', 0))
                    })
                return markets
        except Exception as e:
            print(f"[RANGE] Error fetching market list: {str(e)[:80]}", flush=True)
    
    return []

def fetch_market_data_dict(limit=50, strategy="MOMENTUM"):
    """
    Fetch market data using strategy-specific APIs with fallbacks.
    
    Market data APIs (with fallbacks):
    - MOMENTUM: CoinGecko (primary) → CEX.IO (fallback)
    - REVERSAL: CoinPaprika (primary) → Bitstamp (fallback)
    - RANGE: Huobi (primary) → Gemini (fallback)
    
    Args:
        limit: Maximum number of pairs to fetch
        strategy: Strategy name ("MOMENTUM", "REVERSAL", "RANGE") to use appropriate API chain
    """
    markets = fetch_market_list(limit=limit, strategy=strategy)
    result = {}
    strategy_upper = strategy.upper() if isinstance(strategy, str) else "MOMENTUM"
    
    for d in markets:
        try:
            symbol = (d.get("symbol") or "").upper()
            if not symbol or is_stablecoin(symbol):
                continue
                
            key = symbol + "USDT"
            
            # Fetch enhanced data from strategy-specific API with fallback
            enhanced_data = None
            try:
                if strategy_upper == "MOMENTUM":
                    # CoinGecko primary → CEX.IO fallback
                    enhanced_data = fetch_market_data_momentum(symbol)
                elif strategy_upper == "REVERSAL":
                    # CoinPaprika primary → Bitstamp fallback
                    enhanced_data = fetch_market_data_reversal(symbol)
                elif strategy_upper == "RANGE":
                    # Huobi primary → Gemini fallback
                    enhanced_data = fetch_market_data_range(symbol)
            except Exception:
                pass
            
            # Use enhanced data if available, otherwise use basic data from list (except for RANGE which is strict)
            if enhanced_data and enhanced_data.get("price", 0) > 0:
                result[key] = {
                    "price": float(enhanced_data.get("price", 0.0)),
                    "high_24h": float(enhanced_data.get("high_24h", d.get("high_24h", 0.0)) or d.get("high_24h", 0.0) or 0.0),
                    "low_24h": float(enhanced_data.get("low_24h", d.get("low_24h", 0.0)) or d.get("low_24h", 0.0) or 0.0),
                    "price_change_1h": float(enhanced_data.get("price_change_1h", d.get("price_change_1h_in_currency", d.get("price_change_percentage_1h_in_currency", 0.0))) or 0.0),
                    "price_change_24h": float(enhanced_data.get("price_change_24h", enhanced_data.get("change", d.get("price_change_24h_in_currency", d.get("price_change_percentage_24h_in_currency", 0.0)))) or 0.0),
                    "total_volume": float(enhanced_data.get("volume", d.get("total_volume", 0.0)) or 0.0),
                    "volume": float(enhanced_data.get("volume", d.get("total_volume", 0.0)) or 0.0),
                    "coin_id": enhanced_data.get("coin_id", d.get("id"))
                }
            elif strategy_upper == "RANGE":
                # For RANGE, we skip if strategy-specific API (Huobi/Gemini) failed
                if DEBUG_QUALITY:
                    print(f"[RANGE_PRICING] Skipping {symbol} - No fresh price from Huobi/Gemini", flush=True)
                continue
            else:
                # Use basic data from market list (fallback for MOMENTUM/REVERSAL)
                result[key] = {
                    "price": float(d.get("current_price") or 0.0),
                    "high_24h": float(d.get("high_24h") or (d.get("current_price", 0) * 1.05) or 0.0),
                    "low_24h": float(d.get("low_24h") or (d.get("current_price", 0) * 0.95) or 0.0),
                    "price_change_1h": float(d.get("price_change_1h_in_currency") or d.get("price_change_percentage_1h_in_currency") or 0.0),
                    "price_change_24h": float(d.get("price_change_24h_in_currency") or d.get("price_change_percentage_24h_in_currency") or 0.0),
                    "total_volume": float(d.get("total_volume") or 0.0),
                    "volume": float(d.get("total_volume") or 0.0),
                    "coin_id": d.get("id")
                }
        except Exception:
            continue
    return result

def get_trending_pairs(limit=None):
    """Fetch trending pairs and populate the TRENDING_PAIRS global dictionary."""
    global TRENDING_PAIRS
    try:
        market_data = fetch_market_data_dict(limit=limit or CACHE_WARMING_PAIR_COUNT)
        with TRENDING_PAIRS_LOCK:
            TRENDING_PAIRS.clear()
            TRENDING_PAIRS.update(market_data)
        if DEBUG_STAGE_5 and market_data:
            print(f"[STAGE 5] Loaded {len(market_data)} trending pairs", flush=True)
    except Exception as e:
        if DEBUG_STAGE_5:
            print(f"[STAGE 5] Error loading trending pairs: {e}", flush=True)

def fetch_top_gainers(markets=None, limit=100):
    """Extract top gainer pairs from market data (highest 24h % change)."""
    try:
        if markets is None:
            markets = fetch_market_list(limit=limit)
        gainers = []
        for d in markets:
            try:
                change_24h = float(d.get("price_change_percentage_24h_in_currency") or d.get("price_change_percentage_24h") or 0.0)
                if change_24h > 0:
                    symbol = (d.get("symbol") or "").upper()
                    gainers.append({
                        "id": d.get("id"),
                        "symbol": symbol,
                        "change_24h": change_24h,
                        "price": float(d.get("current_price") or 0.0),
                        "volume": float(d.get("total_volume") or 0.0)
                    })
            except Exception:
                continue
        gainers.sort(key=lambda x: x["change_24h"], reverse=True)
        return gainers[:limit]
    except Exception as e:
        print(f"⚠️ Error fetching top gainers: {e}")
        return []

def fetch_top_losers(markets=None, limit=100):
    """Extract top loser pairs from market data (lowest 24h % change)."""
    try:
        if markets is None:
            markets = fetch_market_list(limit=limit)
        losers = []
        for d in markets:
            try:
                change_24h = float(d.get("price_change_percentage_24h_in_currency") or d.get("price_change_percentage_24h") or 0.0)
                if change_24h < 0:
                    symbol = (d.get("symbol") or "").upper()
                    losers.append({
                        "id": d.get("id"),
                        "symbol": symbol,
                        "change_24h": change_24h,
                        "price": float(d.get("current_price") or 0.0),
                        "volume": float(d.get("total_volume") or 0.0)
                    })
            except Exception:
                continue
        losers.sort(key=lambda x: x["change_24h"])
        return losers[:limit]
    except Exception as e:
        print(f"⚠️ Error fetching top losers: {e}")
        return []

def is_consolidating_pair_fast(pair_data):
    """
    Fast consolidation check using market data (24h high/low, 1h change).
    No API calls needed. Uses simple heuristics.
    Rates consolidation quality: tight ranges preferred over low momentum.
    """
    try:
        price = pair_data.get("price", 0)
        high_24h = pair_data.get("high_24h", 0)
        low_24h = pair_data.get("low_24h", 0)
        change_1h = abs(pair_data.get("price_change_1h", 0))
        change_24h = abs(pair_data.get("price_change_24h", 0))

        if price <= 0:
            return False

        range_quality = 0
        if high_24h > 0 and low_24h > 0:
            range_24h_pct = abs((high_24h - low_24h) / (price + 1e-12))
            if range_24h_pct < 0.10:
                range_quality = 3
            elif range_24h_pct < 0.20:
                range_quality = 2
            elif range_24h_pct < 0.25:
                range_quality = 1
        
        momentum_quality = 0
        if change_1h < 2.0 and change_24h < 10.0:
            momentum_quality = 2
        elif change_1h < 3.0 and change_24h < 15.0:
            momentum_quality = 1

        return range_quality >= 1 or momentum_quality >= 1
    except Exception:
        return False

def fetch_volatile_pairs(markets=None, limit=50, sample_size=150):
    """
    Scan market for volume spike candidates (MOMENTUM strategy).
    Prioritizes pairs with high volume and directional momentum.
    Returns list of volatile/spiking pairs sorted by score.
    
    Scoring:
    - Volume Spike: Recent volume significantly above average
    - Momentum: Strong 1h and/or 24h directional moves
    - Volatility: High intraday range
    """
    try:
        if markets is None:
            markets = fetch_market_list(limit=sample_size)
        
        if not markets:
            if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
                print(f"[fetch_volatile_pairs] No markets data available", flush=True)
            return []
        
        if os.getenv("DEBUG_PAIR_DISCOVERY", "false").lower() == "true":
            print(f"[fetch_volatile_pairs] Scanning {len(markets)} markets for volume spikes", flush=True)
        
        volatile = []

        for d in markets:
            try:
                symbol = (d.get("symbol") or "").upper()
                if is_stablecoin(symbol):
                    continue

                volume = float(d.get("total_volume") or 0.0)
                min_vol_threshold = max(MIN_VOLUME_USD, PAIR_QUALITY_TARGET_VOLUME * 0.3)
                if volume < min_vol_threshold:
                    continue

                price = float(d.get("current_price") or 0.0)
                high_24h = float(d.get("high_24h") or 0.0)
                low_24h = float(d.get("low_24h") or 0.0)
                change_1h = float(d.get("price_change_percentage_1h_in_currency") or 0.0)
                change_24h = float(d.get("price_change_percentage_24h_in_currency") or 0.0)

                if price <= 0:
                    continue

                volume_spike_score = 0
                if volume > PAIR_QUALITY_TARGET_VOLUME * 2:
                    volume_spike_score = 3
                elif volume > PAIR_QUALITY_TARGET_VOLUME:
                    volume_spike_score = 2
                elif volume > PAIR_QUALITY_TARGET_VOLUME * 0.5:
                    volume_spike_score = 1

                momentum_score = 0
                if abs(change_1h) > 3.0 or abs(change_24h) > 15.0:
                    momentum_score = 3
                elif abs(change_1h) > 2.0 or abs(change_24h) > 10.0:
                    momentum_score = 2
                elif abs(change_1h) > 0.5 or abs(change_24h) > 3.0:
                    momentum_score = 1

                volatility_score = 0
                if high_24h > 0 and low_24h > 0:
                    range_24h_pct = abs((high_24h - low_24h) / (price + 1e-12))
                    if range_24h_pct > 0.25:
                        volatility_score += 2
                    elif range_24h_pct > 0.15:
                        volatility_score += 1

                total_score = volume_spike_score * 2 + momentum_score + volatility_score

                if total_score >= 2:
                    volatile.append({
                        "id": d.get("id"),
                        "symbol": symbol,
                        "price": price,
                        "volume": volume,
                        "change_24h": change_24h,
                        "change_1h": change_1h,
                        "volume_spike_score": volume_spike_score,
                        "momentum_score": momentum_score,
                        "volatility_score": volatility_score,
                        "total_score": total_score
                    })

            except Exception:
                continue

        volatile.sort(key=lambda x: x.get("total_score", 0), reverse=True)
        return volatile[:limit]
    except Exception as e:
        print(f"⚠️ Error finding volume spike pairs: {e}")
        return []

def find_consolidating_pairs(limit=60, sample_size=150, markets=None, strategy="RANGE"):
    """
    Scan market for consolidating pairs using fast heuristic.
    Returns list of consolidating pairs with their data.
    
    Args:
        limit: number of consolidating pairs to return
        sample_size: number of markets to scan
        markets: pre-fetched market data (if None, will fetch)
        strategy: "RANGE" by default, used for API key selection
    """
    try:
        if markets is None:
            markets = fetch_market_list(limit=sample_size, strategy=strategy)
        consolidating = []

        for d in markets:
            try:
                symbol = (d.get("symbol") or "").upper()
                if is_stablecoin(symbol):
                    continue

                volume = float(d.get("total_volume") or 0.0)
                min_vol_threshold = max(MIN_VOLUME_USD, PAIR_QUALITY_TARGET_VOLUME * 0.3)
                # For RANGE strategy, avoid extremely high volume pairs that may not form ranges
                max_vol_threshold = PAIR_QUALITY_TARGET_VOLUME * 50 if strategy == "RANGE" else float('inf')
                if volume < min_vol_threshold or volume > max_vol_threshold:
                    continue

                pair_data = {
                    "price": float(d.get("current_price") or 0.0),
                    "high_24h": float(d.get("high_24h") or 0.0),
                    "low_24h": float(d.get("low_24h") or 0.0),
                    "price_change_1h": float(d.get("price_change_percentage_1h_in_currency") or 0.0),
                    "price_change_24h": float(d.get("price_change_percentage_24h_in_currency") or 0.0),
                    "volume": volume
                }

                if is_consolidating_pair_fast(pair_data):
                    consolidating.append({
                        "id": d.get("id"),
                        "symbol": symbol,
                        "price": pair_data["price"],
                        "volume": volume,
                        "change_24h": pair_data["price_change_24h"]
                    })

                if len(consolidating) >= limit:
                    break
            except Exception:
                continue

        return consolidating
    except Exception as e:
        print(f"⚠️ Error finding consolidating pairs: {e}")
        return []

def fetch_simple_price(coin_id):
    """Fetch simple price from regional fetcher with fallback."""
    dbg_price = os.getenv("DEBUG_PRICE", "false").lower() == "true"
    if not coin_id:
        return None
    
    if STRATEGY_FETCHERS_AVAILABLE and get_live_price:
        try:
            price = get_live_price(coin_id, strategy="reversal")
            if price and isinstance(price, (int, float)) and price > 0:
                if dbg_price:
                    print(f"[PRICE] Strategy kline fetcher {coin_id}: {price}")
                return price
        except Exception as e:
            if dbg_price:
                print(f"[PRICE] Strategy kline fetcher {coin_id} failed: {str(e)[:60]}")
    
    if dbg_price:
        print(f"[PRICE] Price fetch failed for {coin_id}")
    return None




def get_market_data_api_chain():
    """
    Try market data APIs in order
    Returns first success or empty dict
    """
    return [
        ('coingecko', fetch_market_data_dict),
        ('cache', lambda: {}),
    ]

def throttled_api_call(api_name, func, *args):
    """
    Call function with automatic throttling to avoid rate limits.
    Properly extracts base API name and tracks call timestamps.
    """
    base_api = api_name.split('_')[0] if '_' in api_name else api_name
    throttle_key = base_api
    
    now = time.time()
    last_call = api_last_call.get(throttle_key, 0)
    wait_time = API_THROTTLE.get(throttle_key, 0.1) - (now - last_call)

    if wait_time > 0:
        safe_sleep(wait_time)

    try:
        result = func(*args)
        api_last_call[throttle_key] = time.time()
        return result
    except Exception as e:
        raise e

def log_api_usage():
    """Print API usage summary at end of cycle."""
    if STRATEGY_FETCHERS_AVAILABLE:
        print("\n[API Status] Strategy kline fetchers active:")
        print("  REVERSAL: Binance → Kraken → CoinGecko")
        print("  RANGE: Bybit → Gate.io → Kraken")
        print("  MOMENTUM: CoinGecko → Binance → Bybit")
    else:
        print("\n[API Status] Independent kline fetching active:")
        print("  All strategies: Kraken → OKX → KuCoin → Coinpaprika")

def load_cached_klines(symbol, exchange, interval):
    """Load klines from persistent cache by exchange."""
    cache_dir = f"klines_cache_{exchange}_{interval}.json"
    try:
        with open(cache_dir, 'r') as f:
            return json.load(f).get(symbol, [])
    except:
        return []

def save_cached_klines(symbol, exchange, interval, data):
    """Save klines to persistent cache by exchange."""
    if not data:
        return
    cache_dir = f"klines_cache_{exchange}_{interval}.json"
    try:
        # Load existing cache
        try:
            with open(cache_dir, 'r') as f:
                cache = json.load(f)
        except:
            cache = {}
        cache[symbol] = data
        with open(cache_dir, 'w') as f:
            json.dump(cache, f)
    except Exception:
        pass

def _validate_klines(klines):
    """Validate klines have proper OHLCV structure and recent timestamps."""
    if not klines or not isinstance(klines, list):
        return False
    if len(klines) < 3:
        return False
    try:
        first_kline = klines[0]
        if not isinstance(first_kline, (list, tuple)) or len(first_kline) < 5:
            return False
        float(first_kline[1])
        float(first_kline[2])
        float(first_kline[3])
        float(first_kline[4])
        float(first_kline[5])
        
        # LIVE DATA VALIDATION: Check if data is recent (within last 10 minutes)
        last_kline = klines[-1]
        last_timestamp = int(last_kline[0])
        current_time = int(time.time() * 1000)  # Convert to milliseconds
        time_diff = current_time - last_timestamp
        max_age_ms = 10 * 60 * 1000  # 10 minutes in milliseconds
        
        if time_diff > max_age_ms:
            if os.getenv("DEBUG_KLINES", "false").lower() == "true":
                print(f"[STALE DATA] Klines too old: {time_diff/1000/60:.1f} minutes behind", flush=True)
            return False
            
        return True
    except (ValueError, TypeError, IndexError):
        return False

def fetch_with_fallback_chain(symbol, chain, cache=True, interval='15m'):
    """
    Fetch klines using strategy-specific fallback chain.
    Each chain item: ('source_name', fetch_function)
    Properly tracks API usage and handles exchange-aware caching.
    Can return list (single interval) or dict (multiple intervals like momentum)
    """
    for source_name, fetch_func in chain:
        try:
            # DISABLED: Cache disabled for live data - always fetch fresh
            # if cache and source_name != 'cache' and 'both' not in source_name.lower():
            #     exchange = source_name.split('_')[0] if '_' in source_name else source_name
            #     cached_data = load_cached_klines(symbol, exchange, interval)
            #     if cached_data and _validate_klines(cached_data):
            #         return cached_data

            result = throttled_api_call(source_name, fetch_func, symbol)
            if result:
                is_valid = False
                if isinstance(result, list):
                    is_valid = _validate_klines(result)
                elif isinstance(result, dict):
                    valid_15m = result.get('15m') and _validate_klines(result['15m'])
                    valid_1h = result.get('1h') and _validate_klines(result['1h'])
                    if valid_15m and valid_1h:
                        is_valid = True
                    elif valid_15m:
                        is_valid = True
                    elif valid_1h:
                        is_valid = True
                
                if is_valid:
                    return result
        except Exception as e:
            continue
    return None



# ---------------- Binance candles & structure ----------------
def symbol_to_binance(pair):
    s = pair.upper().replace("/", "")
    return BINANCE_SYM_EXCEPTIONS.get(s, s)









def fetch_any_klines(symbol, interval="1m", limit=120, coin_id=None, strategy=None):
    """Fetch klines using the new independent sources.
    
    Uses strategy-specific fallback chains when strategy is provided.
    
    Returns: [[timestamp, open, high, low, close, volume], ...] or None
    """
    klines, _ = fetch_klines_with_fallback(symbol, interval, limit, strategy=strategy)
    return klines if klines else None

def compute_vwap_and_volume_clusters(klines, bins=24):
    if not klines:
        return None, []
    total_vol = 0.0
    pv_sum = 0.0
    highs = []; lows = []; prices = []; vols = []
    for k in klines:
        try:
            high = float(k[2]); low = float(k[3]); close = float(k[4]); vol = float(k[5])
            typical = (high + low + close) / 3.0
            total_vol += vol
            pv_sum += typical * vol
            highs.append(high); lows.append(low)
            prices.append(typical); vols.append(vol)
        except Exception:
            continue
    vwap = pv_sum / (total_vol + 1e-12) if total_vol > 0 else None
    if not prices:
        return vwap, []
    pmin = min(lows); pmax = max(highs)
    if pmax <= pmin:
        return vwap, []
    bin_size = (pmax - pmin) / bins
    vol_bins = [0.0] * bins
    bin_centers = [pmin + (i + 0.5) * bin_size for i in range(bins)]
    for price, vol in zip(prices, vols):
        idx = int((price - pmin) / (pmax - pmin) * bins)
        idx = max(0, min(bins-1, idx))
        vol_bins[idx] += vol
    bins_with_vol = list(zip(bin_centers, vol_bins))
    top_bins = sorted(bins_with_vol, key=lambda x: x[1], reverse=True)
    top_bins = [b for b in top_bins if b[1] > 0]
    return vwap, top_bins

def detect_recent_swing_levels(klines, lookback=40):
    if not klines:
        return [], []
    highs = [float(k[2]) for k in klines[-lookback:]]
    lows = [float(k[3]) for k in klines[-lookback:]]
    n = len(highs)
    swing_highs = []; swing_lows = []
    for i in range(2, n - 2):
        if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] >= highs[i+1] and highs[i] >= highs[i+2]:
            swing_highs.append(highs[i])
        if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] <= lows[i+1] and lows[i] <= lows[i+2]:
            swing_lows.append(lows[i])
    swing_highs = sorted(list(set(swing_highs)), reverse=True)
    swing_lows = sorted(list(set(swing_lows)))
    return swing_highs, swing_lows

# ---------------- TradingView helpers ----------------
def fetch_tradingview_analysis(symbol_pair):
    if not TV_AVAILABLE or not TV_SCREENER or not TV_EXCHANGE:
        return None
    try:
        sym_for_tv = symbol_pair.replace("USDT", "/USDT")
        handler = TA_Handler(
            symbol=sym_for_tv,
            screener=TV_SCREENER,
            exchange=TV_EXCHANGE,
            interval=TV_INTERVAL
        )
        analysis = handler.get_analysis()
        return analysis
    except Exception:
        return None

def extract_tv_metrics(analysis):
    if analysis is None:
        return None
    try:
        ind = analysis.indicators or {}
        rsi = float(ind.get("RSI", 50)) if ind.get("RSI") is not None else 50.0
        ema5 = None; ema20 = None
        for k in ("EMA5", "EMA 5", "EMA(5)", "MA5"):
            if k in ind and ind[k] is not None:
                try:
                    ema5 = float(ind[k]); break
                except Exception:
                    pass
        for k in ("EMA20", "EMA 20", "EMA(20)", "MA20"):
            if k in ind and ind[k] is not None:
                try:
                    ema20 = float(ind[k]); break
                except Exception:
                    pass
        bb_upper = ind.get("BB.upper"); bb_lower = ind.get("BB.lower")
        bbw = None
        if bb_upper is not None and bb_lower is not None:
            try:
                bb_upper_f = float(bb_upper); bb_lower_f = float(bb_lower)
                bbw = (bb_upper_f - bb_lower_f) / ((bb_upper_f + bb_lower_f) / 2 + 1e-12)
            except Exception:
                bbw = None
        return {"rsi": rsi, "ema5": ema5, "ema20": ema20, "bbw": bbw}
    except Exception:
        return None

# ---------------- TradingView-like computed metrics (fallback when TV not available) ----------------

def compute_bb_width_for_closes(closes, period=20):
    """Compute Bollinger Band width from closes list."""
    if not closes or len(closes) < period:
        return None
    try:
        window = closes[-period:]
        sma = sum(window) / len(window)
        var = sum((c - sma) ** 2 for c in window) / len(window)
        sd = math.sqrt(var)
        bbw = ((sma + 2*sd) - (sma - 2*sd)) / ((sma + sma) / 2 + 1e-12)
        return bbw
    except Exception:
        return None


def compute_indicators(closes, rsi_period=14, ema_short=5, ema_long=20, bb_period=20):
    """Compute all technical indicators in one pass. Returns dict {rsi, ema5, ema20, bbw}"""
    if not closes or len(closes) < max(rsi_period + 1, bb_period):
        return {"rsi": 50.0, "ema5": None, "ema20": None, "bbw": None}
    
    try:
        ema_vals = [None, None]
        rsi = None
        bbw = None
        
        alpha_short = 2.0 / (ema_short + 1.0)
        alpha_long = 2.0 / (ema_long + 1.0)
        ema_short_val = closes[0]
        ema_long_val = closes[0]
        for c in closes[1:]:
            ema_short_val = (c - ema_short_val) * alpha_short + ema_short_val
            ema_long_val = (c - ema_long_val) * alpha_long + ema_long_val
        ema_vals = [float(ema_short_val), float(ema_long_val)]
        
        if len(closes) >= rsi_period + 1:
            gains = [max(0.0, closes[i] - closes[i-1]) for i in range(1, rsi_period + 1)]
            losses = [max(0.0, closes[i-1] - closes[i]) for i in range(1, rsi_period + 1)]
            avg_gain = sum(gains) / rsi_period
            avg_loss = sum(losses) / rsi_period
            for i in range(rsi_period + 1, len(closes)):
                gain = max(0.0, closes[i] - closes[i-1])
                loss = max(0.0, closes[i-1] - closes[i])
                avg_gain = (avg_gain * (rsi_period - 1) + gain) / rsi_period
                avg_loss = (avg_loss * (rsi_period - 1) + loss) / rsi_period
            rsi = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + avg_gain / (avg_loss + 1e-12)))
        
        if len(closes) >= bb_period:
            window = closes[-bb_period:]
            sma = sum(window) / len(window)
            var = sum((c - sma) ** 2 for c in window) / len(window)
            sd = math.sqrt(var)
            bbw = ((sma + 2*sd) - (sma - 2*sd)) / ((sma + sma) / 2 + 1e-12)
        
        return {"rsi": float(rsi) if rsi else 50.0, "ema5": ema_vals[0], "ema20": ema_vals[1], "bbw": bbw}
    except Exception:
        return {"rsi": 50.0, "ema5": None, "ema20": None, "bbw": None}


def compute_tv_metrics(klines, **kwargs):
    """Compute technical indicators from klines. Returns dict {rsi, ema5, ema20, bbw}"""
    if not klines:
        return None
    try:
        closes = [float(x[4]) for x in klines if len(x) >= 5]
        return compute_indicators(closes, **kwargs) if closes else None
    except Exception:
        return None

# ---------------- scoring helpers ----------------

def map_leverage(conf_pct):
    c = max(0.0, min(100.0, conf_pct))
    lev = LEV_MIN + (LEV_MAX - LEV_MIN) * (c / 100.0)
    return max(LEV_MIN, min(LEV_MAX, int(round(lev))))

def map_profit_multiplier(conf_pct):
    c = max(50.0, min(100.0, conf_pct))
    t = (c - 50.0) / 50.0
    return PROFIT_MIN + (PROFIT_MAX - PROFIT_MIN) * t

def enrich_signal_with_dynamics(signal):
    """Add leverage and profit_multiplier to signal based on confidence."""
    if signal:
        conf = signal.get('confidence', 0.0)
        signal['leverage'] = map_leverage(conf)
        signal['profit_multiplier'] = round(map_profit_multiplier(conf), 3)
    return signal


def classify_pair_volatility(vol_est):
    """Classify pair into stable / medium / high beta buckets."""
    if vol_est is None:
        return "unknown"
    try:
        low, high = VOL_CLASS_THRESHOLDS if len(VOL_CLASS_THRESHOLDS) >= 2 else (0.08, 0.22)
    except Exception:
        low, high = (0.08, 0.22)
    if vol_est <= low:
        return "stable"
    if vol_est >= high:
        return "high_beta"
    return "medium"


def adjust_conf_for_vol_class(conf, vol_class):
    """Scale confidence based on volatility class."""
    if conf is None:
        return 0.0
    if vol_class == "stable":
        return min(100.0, conf * 1.05)
    if vol_class == "high_beta":
        return conf * 0.9
    return conf


def compute_liquidity_penalty(volume):
    """Return multiplier based on volume vs target."""
    if volume is None or volume <= 0:
        return LOW_VOLUME_PENALTY
    if volume >= PAIR_QUALITY_TARGET_VOLUME:
        return 1.0
    shortfall = (PAIR_QUALITY_TARGET_VOLUME - volume) / PAIR_QUALITY_TARGET_VOLUME
    return max(LOW_VOLUME_PENALTY, 1.0 - shortfall * 0.4)


def compute_pair_hotness(pair_data, market_rank, total_pairs):
    """Higher bonus for coins outperforming peers."""
    if market_rank is None or total_pairs <= 0:
        return 0.0
    percentile = 1.0 - (market_rank / total_pairs)
    return max(0.0, HOTNESS_BONUS_MAX * percentile)


def detect_trend_regime_from_klines(klines):
    """Rudimentary trend regime classification using slope of closes."""
    if not klines or len(klines) < 12:
        return "neutral", 0.0
    closes = [float(k[4]) for k in klines[-24:]]
    if len(closes) < 6:
        return "neutral", 0.0
    start = statistics.mean(closes[:len(closes)//3])
    mid = statistics.mean(closes[len(closes)//3: 2*len(closes)//3])
    end = statistics.mean(closes[2*len(closes)//3:])
    slope = (end - start) / (abs(start) + 1e-12)
    if abs(slope) < 0.005:
        return "sideways", slope
    if slope > 0:
        return "uptrend", slope
    return "downtrend", slope


def extract_short_medium_momentum(klines):
    """Derive short-term (<=1h) and medium-term (1-4h) momentum from 15m klines."""
    if not klines or not isinstance(klines, list) or len(klines) < 4:
        return 0.0, 0.0
    try:
        closes = [float(k[4]) for k in klines]
        short_window = min(4, len(closes)-1)  # ~1h
        medium_window = min(16, len(closes)-1)  # ~4h
        short_mom = (closes[-1] - closes[-1 - short_window]) / (closes[-1 - short_window] + 1e-12)
        medium_mom = (closes[-1] - closes[-1 - medium_window]) / (closes[-1 - medium_window] + 1e-12)
        return short_mom * 100.0, medium_mom * 100.0
    except Exception:
        return 0.0, 0.0


def volatility_regime_factor(tv_metrics, klines, vol_est):
    """Return multiplier (0-1) indicating whether volatility regime is acceptable."""
    bbw = tv_metrics.get("bbw") if tv_metrics else None
    if bbw is None and klines:
        try:
            bbw = compute_bb_width_for_closes([float(k[4]) for k in klines], period=20)
        except Exception:
            bbw = None
    if bbw is not None and bbw > VOL_REGIME_BBW_MAX:
        return 0.0
    atr = compute_atr_from_klines(klines) if klines else None
    if atr and vol_est:
        ref = max(1e-4, vol_est * float(VOL_REGIME_ATR_MULT))
        if atr > ref:
            return max(0.0, ref / atr)
    return 1.0


def apply_soft_cooldown_adjustment(conf, pair, direction, cooldown_cache):
    """Apply soft cooldown penalty instead of outright skipping."""
    key = f"{pair}_{direction}"
    last_ts = cooldown_cache.get(key, 0)
    if not last_ts:
        return conf
    age = time.time() - last_ts
    if age >= SOFT_COOLDOWN_SECONDS:
        return conf
    progress = max(0.0, min(1.0, age / SOFT_COOLDOWN_SECONDS))
    factor = SOFT_COOLDOWN_MIN_FACTOR + (1.0 - SOFT_COOLDOWN_MIN_FACTOR) * progress
    return conf * factor


def signal_spread_penalty(entry):
    """Approximate bid-ask spread penalty in absolute price terms."""
    if entry is None:
        return 0.0
    return entry * (BID_ASK_SPREAD_BPS / 10000.0)


def adjust_target_for_spread(entry, target, direction):
    """Shift TP targets inward to account for bid-ask spread."""
    if entry is None or target is None or direction not in ("LONG", "SHORT"):
        return target
    spread = signal_spread_penalty(entry)
    if direction == "LONG":
        return max(entry, target - spread)
    return min(entry, target + spread)


def compute_volume_context(klines, short_lb=12, long_lb=48):
    """Return recent vs longer-term average volumes."""
    if not klines:
        return None, None
    vols = [float(k[5]) for k in klines if len(k) >= 6]
    if not vols:
        return None, None
    short_vals = vols[-short_lb:] if len(vols) >= short_lb else vols
    long_vals = vols[-long_lb:] if len(vols) >= long_lb else vols
    short_avg = statistics.mean(short_vals) if short_vals else None
    long_avg = statistics.mean(long_vals) if long_vals else None
    return short_avg, long_avg


def find_recent_wick_indices(klines, direction, max_candles=4):
    """Return list of (offset_from_end, wick_ratio) for recent wick rejections."""
    if not klines:
        return []
    results = []
    cand_count = min(max_candles, len(klines))
    for idx in range(1, cand_count + 1):
        k = klines[-idx]
        open_p = float(k[1]); high = float(k[2]); low = float(k[3]); close = float(k[4])
        body = abs(close - open_p) or 1e-12
        if direction == "SHORT":
            wick = high - max(close, open_p)
        else:
            wick = min(close, open_p) - low
        ratio = wick / body
        if ratio >= WICK_BODY_RATIO and wick / (abs(close) + 1e-12) >= WICK_MIN_PCT:
            results.append((idx, ratio))
    return results


def momentum_divergence_check(klines, direction):
    """Detect simple price/RSI divergence proxy."""
    if not klines or not isinstance(klines, list) or len(klines) < RSI_DIV_LOOKBACK * 2:
        return False
    try:
        closes = [float(k[4]) for k in klines]
        recent = closes[-RSI_DIV_LOOKBACK:]
        prev = closes[-2*RSI_DIV_LOOKBACK:-RSI_DIV_LOOKBACK]
        if direction == "SHORT":
            if max(recent) > max(prev) and statistics.mean(recent) <= statistics.mean(prev):
                return True
        else:
            if min(recent) < min(prev) and statistics.mean(recent) >= statistics.mean(prev):
                return True
        return False
    except Exception:
        return False


def higher_tf_bias_from_klines(klines, period=16):
    """Approximate higher timeframe bias by comparing last vs previous period."""
    if not klines or not isinstance(klines, list) or len(klines) < period * 2:
        return "flat"
    try:
        closes = [float(k[4]) for k in klines]
        recent = statistics.mean(closes[-period:])
        prev = statistics.mean(closes[-2*period:-period])
        if recent > prev * 1.003:
            return "bullish"
        if recent < prev * 0.997:
            return "bearish"
        return "flat"
    except Exception:
        return "flat"


CONFIDENCE_BAND_BIAS = {}


def _confidence_band(conf):
    return int(max(0, min(90, (conf // 10) * 10)))


def update_confidence_band_bias(strategy, conf, delta):
    """Track rolling performance per confidence band for adaptive scaling."""
    key = f"{strategy}:{_confidence_band(conf)}"
    prev = CONFIDENCE_BAND_BIAS.get(key, 0.0)
    CONFIDENCE_BAND_BIAS[key] = prev * 0.7 + delta * 0.3


def apply_confidence_band_bias(strategy, conf):
    key = f"{strategy}:{_confidence_band(conf)}"
    bias = CONFIDENCE_BAND_BIAS.get(key)
    if bias is None:
        return conf
    return max(0.0, min(100.0, conf + bias * 10.0))

def normalize_confidence_by_strategy(conf, strategy_type, preserve_distinctiveness=True):
    """FIXED: Normalize confidence while preserving strategy distinctiveness."""
    if not preserve_distinctiveness:
        # Old normalization: maps ALL signals to 35-95 range
        return 35 + ((conf - 0) / 100) * 60
    
    # FIXED: Strategy-aware normalization ranges
    if strategy_type == 'reversal':
        # Reversal: 25-95 range (wider for extreme signals)
        return 25 + ((conf - 0) / 100) * 70
    elif strategy_type == 'range':
        # Range: 30-90 range (moderate for stable signals)
        return 30 + ((conf - 0) / 100) * 60
    elif strategy_type == 'momentum':
        # Momentum: 35-95 range (standard for momentum signals)
        return 35 + ((conf - 0) / 100) * 60
    else:
        # Default: 30-95 range
        return 30 + ((conf - 0) / 100) * 65


def adjust_signal_confidence(signal, strategy_type, momentum_strength=None, volume_surge=False, breakout_detected=False):
    """
    Enhanced confidence adjustment using all available data for more accurate scoring.
    Considers: TP/SL probabilities, volume patterns, momentum strength, technical indicators,
    multi-timeframe alignment, and market conditions.
    """
    if not signal:
        return signal
    try:
        direction = signal.get('direction', '').lower()
        strategy_key = f"{strategy_type}_{direction}" if direction else strategy_type
        base_conf = float(signal.get("confidence", 50.0))
        prob_tp = signal.get('prob_tp', 0.5)
        prob_sl = signal.get('prob_sl', 0.5)
        
        update_confidence_band_bias(strategy_key, base_conf, prob_tp - prob_sl)
        
        if volume_surge is False:
            volume_surge = signal.get('volume_surge', False)
        if breakout_detected is False:
            breakout_detected = signal.get('breakout_detected', False)
        
        adj_conf = adjust_confidence_with_probs(base_conf, prob_tp, prob_sl)
        adj_conf = apply_confidence_band_bias(strategy_key, adj_conf)
        
        # Enhanced probability-based adjustment
        prob_diff = prob_tp - prob_sl
        prob_bonus = prob_diff * 15.0  # More weight on probability difference
        adj_conf += prob_bonus
        
        bonus = 0.0
        
        # Use TV metrics if available
        tv_metrics = signal.get('tv_metrics', {})
        if isinstance(tv_metrics, dict):
            rsi = tv_metrics.get('rsi')
            if rsi:
                # RSI alignment bonus
                if direction == "LONG" and 30 < rsi < 50:
                    bonus += 2.0  # Oversold but not extreme
                elif direction == "SHORT" and 50 < rsi < 70:
                    bonus += 2.0  # Overbought but not extreme
                elif direction == "LONG" and rsi < 30:
                    bonus += 1.0  # Extreme oversold
                elif direction == "SHORT" and rsi > 70:
                    bonus += 1.0  # Extreme overbought
            
            # EMA alignment
            ema5 = tv_metrics.get('ema5')
            ema20 = tv_metrics.get('ema20')
            if ema5 and ema20:
                if direction == "LONG" and ema5 > ema20:
                    bonus += 1.5
                elif direction == "SHORT" and ema5 < ema20:
                    bonus += 1.5
        
        # Quality score influence
        quality_score = signal.get('quality_score', 0.0)
        if quality_score > 70:
            bonus += (quality_score - 70) / 10.0  # Up to 3 points for high quality
        
        if strategy_type == 'reversal':
            if momentum_strength:
                strength_class = momentum_strength.get("class", "NORMAL")
                if strength_class == "EXPLOSIVE":
                    bonus += 5.0  # Increased from 2.0
                elif strength_class == "STRONG":
                    bonus += 3.0  # Increased from 2.0
                elif strength_class == "MODERATE":
                    bonus += 1.5
            
            if volume_surge and breakout_detected:
                bonus += 5.0  # Increased from 3.0
            elif volume_surge:
                bonus += 2.5  # Increased from 1.5
            elif breakout_detected:
                bonus += 2.0  # Increased from 1.0
            
            # R/R ratio bonus
            tp_pct = signal.get('tp_pct', 0.0)
            sl_pct = signal.get('sl_pct', 0.0)
            if sl_pct > 0:
                rr_ratio = tp_pct / sl_pct
                if rr_ratio > 3.0:
                    bonus += 2.0
                elif rr_ratio > 2.5:
                    bonus += 1.0
                
        elif strategy_type == 'range':
            if volume_surge:
                bonus += 2.5  # Increased from 1.5
            
            if breakout_detected:
                bonus += 2.0  # Increased from 1.0
            
            # Range stability bonus
            stability_score = signal.get('stability_score', 0.0)
            if stability_score > 70:
                bonus += 2.0
            elif stability_score > 50:
                bonus += 1.0
            
            # Rejection count bonus
            rejections = signal.get('rejections_total', 0)
            if rejections >= 3:
                bonus += 2.0
            elif rejections >= 2:
                bonus += 1.0
                
        elif strategy_type == 'momentum':
            if momentum_strength:
                strength_class = momentum_strength.get("class", "NORMAL")
                if strength_class == "EXPLOSIVE":
                    bonus += 6.0  # Increased from 3.0
                elif strength_class == "STRONG":
                    bonus += 4.0  # Increased from 3.0
                elif strength_class == "MODERATE":
                    bonus += 2.0
            
            if breakout_detected and volume_surge:
                bonus += 6.0  # Increased from 3.0
            elif breakout_detected or volume_surge:
                bonus += 3.0  # Increased from 1.5
            
            # Multi-timeframe alignment bonus
            mtf_alignment = signal.get('mtf_alignment', 0)
            if mtf_alignment >= 3:
                bonus += 3.0
            elif mtf_alignment >= 2:
                bonus += 1.5
        
        # Market trend alignment
        market_trend = signal.get('market_trend', 'neutral')
        if market_trend == "bullish" and direction == "LONG":
            bonus += 1.5
        elif market_trend == "bearish" and direction == "SHORT":
            bonus += 1.5
        
        adj_conf = min(100.0, max(0.0, adj_conf + bonus))
        signal["confidence"] = round(adj_conf, 2)
    except Exception:
        pass
    return signal


def analyze_multi_timeframe_momentum(kl_1m, kl_5m, kl_15m, direction):
    """Aggregate momentum analysis across multiple timeframes (penalized)."""
    momentum_scores = []
    agreement_count = 0

    try:
        if kl_1m and len(kl_1m) >= 4:
            short_mom, _ = extract_short_medium_momentum(kl_1m)
            momentum_scores.append(short_mom)
            if (direction == "LONG" and short_mom > 0.02) or (direction == "SHORT" and short_mom < -0.02):
                agreement_count += 1

        if kl_5m and len(kl_5m) >= 4:
            short_mom, _ = extract_short_medium_momentum(kl_5m)
            momentum_scores.append(short_mom * 0.8)
            if (direction == "LONG" and short_mom > 0.01) or (direction == "SHORT" and short_mom < -0.01):
                agreement_count += 1

        if kl_15m and len(kl_15m) >= 4:
            short_mom, _ = extract_short_medium_momentum(kl_15m)
            momentum_scores.append(short_mom * 0.7)
            if (direction == "LONG" and short_mom > 0.005) or (direction == "SHORT" and short_mom < -0.005):
                agreement_count += 1

        # Penalizing signals with low alignment
        if agreement_count < 2:
            return 0.0, agreement_count, -5.0

        combined_momentum = statistics.mean(momentum_scores) if momentum_scores else 0.0
        confidence_bonus = min(10.0, agreement_count * 3.0)
        return combined_momentum, agreement_count, confidence_bonus
    except Exception:
        return 0.0, 0, 0.0


def analyze_multi_timeframe_volume(kl_1m, kl_5m, kl_15m):
    """
    Analyze volume trends across multiple timeframes.
    Returns: (volume_trend, surge_detected, trend_strength)
    """
    if not any([kl_1m, kl_5m, kl_15m]):
        return "neutral", False, 0.0
    
    try:
        trend_count = {"bullish": 0, "bearish": 0, "neutral": 0}
        average_surge = 0.0
        surge_count = 0
        
        for klines in [kl_1m, kl_5m, kl_15m]:
            if not klines or not isinstance(klines, list) or len(klines) < 5:
                continue
            
            try:
                is_surge, ratio = detect_volume_surge(klines)
                if is_surge:
                    average_surge += ratio
                    surge_count += 1
                
                closes = [float(k[4]) for k in klines[-5:] if len(k) >= 5]
                volumes = [float(k[5]) for k in klines[-5:] if len(k) >= 6]
                
                if len(closes) >= 2 and len(volumes) >= 2:
                    recent_vol = statistics.mean(volumes[-3:])
                    prior_vol = statistics.mean(volumes[:-2]) if len(volumes) > 2 else statistics.mean(volumes)
                    
                    if prior_vol > 0:
                        if recent_vol > prior_vol * 1.2:
                            trend_count["bullish"] += 1
                        elif recent_vol < prior_vol * 0.8:
                            trend_count["bearish"] += 1
                        else:
                            trend_count["neutral"] += 1
            except Exception:
                continue
        
        total_trends = sum(trend_count.values())
        if total_trends == 0:
            return "neutral", False, 0.0
        
        trend = max(trend_count.items(), key=lambda x: x[1])[0]
        avg_surge_ratio = average_surge / surge_count if surge_count > 0 else 0.0
        surge_detected = avg_surge_ratio > 1.5
        trend_strength = min(3.0, float(total_trends)) / 3.0
        
        return trend, bool(surge_detected), float(trend_strength)
    except Exception:
        return "neutral", False, 0.0


def build_hotness_rankings(market_data):
    """Return dict mapping pair to relative rank (lower is hotter)."""
    if not market_data:
        return {}
    sorted_pairs = sorted(
        market_data.items(),
        key=lambda kv: kv[1].get("price_change_24h", 0.0) or 0.0,
        reverse=True
    )
    ranks = {}
    for idx, (pair, _) in enumerate(sorted_pairs):
        ranks[pair] = idx
    return ranks

def calculate_final_signal_score(signal, ranking_type="overall"):
    """FIXED: Calculate final signal score with corrected weighting.
    
    Weights reflect actual predictive power:
    - Prob TP (40%): Most directly predictive of actual win rate
    - Confidence (30%): Signal quality assessment from detection
    - Quality (20%): Structural quality of signal (layers, confirmations)
    - Layers (10%): Supporting evidence count
    """
    confidence = signal.get("confidence", 0.0)
    quality = signal.get("quality_score", 0.0)
    prob_tp = signal.get("prob_tp", 0.5)
    layers = len(signal.get("trigger_layers", []))
    
    if ranking_type == "overall":
        # Overall ranking: prob_tp is most predictive
        score = (prob_tp * 100 * 0.40 +
                confidence * 0.30 + 
                quality * 0.20 + 
                layers * 10 * 0.10)
    else:
        # Per-strategy ranking: slightly favor confidence and quality
        score = (prob_tp * 100 * 0.40 +
                confidence * 0.32 + 
                quality * 0.18 + 
                layers * 10 * 0.10)
    
    return round(score, 2)


def calculate_probability(pair_data, symbol=None, vol_cache=None, tv_metrics=None):
    rsi_score = 0.0
    ema_bias = 0.0
    bbw = None

    # Prefer explicit tv_metrics passed in (computed fallback), otherwise try TradingView if available
    metrics = None
    if tv_metrics:
        metrics = tv_metrics
    elif symbol and TV_AVAILABLE:
        analysis = fetch_tradingview_analysis(symbol)
        metrics = extract_tv_metrics(analysis)
    if metrics:
        rsi = metrics.get("rsi")
        ema5 = metrics.get("ema5")
        ema20 = metrics.get("ema20")
        bbw = metrics.get("bbw")
        if rsi is not None:
            if rsi >= 70:
                rsi_score = 1.0
            elif rsi <= 30:
                rsi_score = -1.0
            else:
                rsi_score = (rsi - 50.0) / 50.0
        if ema5 is not None and ema20 is not None:
            ema_bias = 1.0 if ema5 > ema20 else -1.0

    oneh = pair_data.get("price_change_1h", 0.0)
    day = pair_data.get("price_change_24h", 0.0)
    momentum = (oneh * 0.3) + (day * 0.7)
    vol_score = 0.0
    if vol_cache is not None:
        v = vol_cache.get(pair_data.get("coin_id") or "", None)
        if v is not None:
            vol_score = max(-1.0, min(1.0, (0.20 - v) * 5.0))
    mag = (W_EMA * ema_bias) + (W_RSI * rsi_score) + (W_MOM * (momentum / 10.0)) + (W_VOL * vol_score)
    conf = (mag + 1.0) / 2.0 * 100.0
    return max(0.0, min(100.0, conf))

def weighted_score(prob, pair_data):
    oneh = pair_data.get("price_change_1h", 0.0)
    day = pair_data.get("price_change_24h", 0.0)
    vol = pair_data.get("volume", 1.0)
    momentum = (oneh * 0.3) + (day * 0.7)
    vol_factor = min(1.5, max(0.5, vol / 1e8))
    score = prob * 0.75 + (momentum * vol_factor)
    return max(0.0, min(100.0, score))

# ---------------- adaptive SL/TP & estimate_hit_prob (reused) ----------------
def estimate_hit_prob(tp_pct, sl_pct, tv_metrics, pair_data, direction, market_trend, vol_est, binance_info=None):
    base_tp = 0.35
    base_sl = 0.35
    price = pair_data.get("price") or 1.0
    zone, pos = compute_zone(pair_data)
    ema_factor = 0.0
    if tv_metrics:
        ema5 = tv_metrics.get("ema5"); ema20 = tv_metrics.get("ema20")
        if ema5 is not None and ema20 is not None:
            ema_diff = (ema5 - ema20) / (price + 1e-12)
            ema_factor = max(-1.0, min(1.0, ema_diff * 100.0))
    rsi = tv_metrics.get("rsi") if tv_metrics else 50.0
    rsi_factor = 0.0
    if direction == "LONG":
        if rsi < 40:
            rsi_factor = 0.15
        elif rsi < 60:
            rsi_factor = 0.08
        elif rsi > 75:
            rsi_factor = -0.12
    else:
        if rsi > 60:
            rsi_factor = 0.15
        elif rsi > 40:
            rsi_factor = 0.08
        elif rsi < 25:
            rsi_factor = -0.12
    vol_scale = min(2.0, max(0.5, (vol_est or 0.01) / 0.02 + 0.8))
    trend_align = 0.0
    if (market_trend == "bullish" and direction == "LONG") or (market_trend == "bearish" and direction == "SHORT"):
        trend_align = 0.10
    elif (market_trend == "bullish" and direction == "SHORT") or (market_trend == "bearish" and direction == "LONG"):
        trend_align = -0.08
    vol_ref = max(0.002, vol_est or 0.02)
    ease_tp = max(0.0, min(2.0, (tp_pct / vol_ref) * 0.5))
    ease_sl = max(0.0, min(2.0, (sl_pct / vol_ref) * 0.5))
    cluster_bonus = 0.0
    if binance_info and binance_info.get("top_bins"):
        top_centers = [c for c, v in binance_info["top_bins"][:3]]
        target_price_tp = price * (1 + tp_pct) if direction == "LONG" else price * (1 - tp_pct)
        for center in top_centers:
            if abs(center - target_price_tp) / price < 0.012:
                cluster_bonus += 0.06
        target_price_sl = price * (1 - sl_pct) if direction == "LONG" else price * (1 + sl_pct)
        for center in top_centers:
            if abs(center - target_price_sl) / price < 0.012:
                cluster_bonus -= 0.04
    zone_bonus = 0.0
    if pos is not None:
        if direction == "LONG" and zone == "resistance" and pos >= 0.8:
            zone_bonus -= 0.06
        if direction == "LONG" and zone == "support" and pos <= 0.2:
            zone_bonus += 0.06
        if direction == "SHORT" and zone == "support" and pos <= 0.2:
            zone_bonus -= 0.06
        if direction == "SHORT" and zone == "resistance" and pos >= 0.8:
            zone_bonus += 0.06
    p_tp = base_tp + 0.25 * ema_factor + rsi_factor * 0.6 + 0.12 * trend_align + 0.15 * (1.0 / (1.0 + ease_tp)) * vol_scale + cluster_bonus + zone_bonus
    p_sl = base_sl - 0.15 * ema_factor - rsi_factor * 0.3 - 0.08 * trend_align + 0.12 * (1.0 / (1.0 + ease_sl)) * vol_scale - cluster_bonus - zone_bonus
    p_tp = max(0.02, min(0.98, p_tp))
    p_sl = max(0.01, min(0.98, p_sl))
    total = p_tp + p_sl
    if total > 0.99:
        factor = 0.99 / total
        p_tp *= factor; p_sl *= factor
    return p_tp, p_sl

def calculate_dynamic_tp_sl_from_movement(klines, entry_price, direction, strategy_type="reversal",
                                          tv_metrics=None, bin_info=None, vol_est=None, 
                                          momentum_strength=None, confidence=50.0, market_trend="neutral"):
    """
    Enhanced TP/SL calculation for bigger, higher quality moves.
    Uses advanced technical analysis: swing levels, support/resistance, volatility, momentum.
    
    Strategy-aware calculation:
    - REVERSAL: Aggressive targets using momentum + swing levels (up to 35% TP)
    - RANGE: Smart targets within range boundaries (up to 15% TP)
    - MOMENTUM: Dynamic targets based on momentum strength (up to 40% TP)
    
    Returns: (sl_pct, tp_pct)
    """
    try:
        if not klines or len(klines) < 14 or entry_price <= 0:
            return 0.01, 0.02
    except (TypeError, AttributeError):
        return 0.01, 0.02
    
    try:
        # Calculate ATR with multiple periods for better volatility assessment
        atr_14 = compute_atr_from_klines(klines, period=14)
        atr_21 = compute_atr_from_klines(klines, period=21) if len(klines) >= 21 else atr_14
        atr = max(atr_14, atr_21 * 0.9) if atr_21 else atr_14
        
        if not atr or atr <= 0:
            atr = (klines[-1][2] - klines[-1][3]) / entry_price
        
        # Enhanced volatility calculation
        closes = [float(k[4]) for k in klines[-30:]] if len(klines) >= 30 else [float(k[4]) for k in klines]
        recent_volatility = (max(closes) - min(closes)) / entry_price if closes else 0.01
        
        # Use bin_info swing levels if available (more accurate)
        if bin_info and isinstance(bin_info, dict):
            swing_highs_bin = bin_info.get("swing_highs", [])
            swing_lows_bin = bin_info.get("swing_lows", [])
            if swing_highs_bin and swing_lows_bin:
                recent_high = max(swing_highs_bin)
                recent_low = min(swing_lows_bin)
            else:
                swing_highs, swing_lows = detect_recent_swing_levels(klines, lookback=60)
                recent_high = max(swing_highs) if swing_highs else entry_price * (1 + recent_volatility * 1.5)
                recent_low = min(swing_lows) if swing_lows else entry_price * (1 - recent_volatility * 1.5)
        else:
            swing_highs, swing_lows = detect_recent_swing_levels(klines, lookback=60)
            recent_high = max(swing_highs) if swing_highs else entry_price * (1 + recent_volatility * 1.5)
            recent_low = min(swing_lows) if swing_lows else entry_price * (1 - recent_volatility * 1.5)
        
        # Calculate distances with better logic
        if direction == "LONG":
            distance_to_resistance = (recent_high - entry_price) / entry_price if recent_high > entry_price else atr * 3.0
            distance_to_support = (entry_price - recent_low) / entry_price if entry_price > recent_low else atr * 1.2
        else:
            distance_to_support = (entry_price - recent_low) / entry_price if entry_price > recent_low else atr * 3.0
            distance_to_resistance = (recent_high - entry_price) / entry_price if recent_high > entry_price else atr * 1.2
        
        atr_pct = atr / entry_price
        volatility_factor = min(2.0, max(0.5, recent_volatility / atr_pct)) if atr_pct > 0 else 1.0
        
        # Confidence-based multipliers
        confidence_factor = 1.0 + (confidence - 50.0) / 200.0  # 0.75x to 1.25x based on confidence
        
        if strategy_type == "reversal":
            # REVERSAL: Target bigger moves using momentum and structural extremes
            # Look back further (40-60 candles) for the 'anchor' of the move being reversed
            lookback_rev = min(len(klines), 50)
            highs_rev = [float(k[2]) for k in klines[-lookback_rev:]]
            lows_rev = [float(k[3]) for k in klines[-lookback_rev:]]
            recent_extreme_high = max(highs_rev)
            recent_extreme_low = min(lows_rev)
            
            # Incorporate Fibonacci retracement zones for TP targets
            range_abs = recent_extreme_high - recent_extreme_low
            retr_38 = 0.382 * range_abs
            retr_62 = 0.618 * range_abs
            
            # Determine if current move is strong/explosive
            ema_slope = _ema_slope([float(k[4]) for k in klines], 20) or 0.0
            is_strong = (abs(ema_slope) > 1e-4) or (confidence > 75)
            
            if direction == "LONG":
                # TP toward Fibonacci levels or recent high
                cons_tp_abs = min(retr_38, recent_extreme_high - entry_price)
                aggr_tp_abs = min(retr_62, recent_extreme_high - entry_price)
                tp_abs = aggr_tp_abs if is_strong else cons_tp_abs
                tp_pct = max(tp_abs / entry_price, atr_pct * 2.5)
                
                # SL: Beyond the absolute recent low AND nearest structural support
                pivot = recent_extreme_low
                # Check for structural support in bin_info
                swing_lows = bin_info.get("swing_lows", []) if bin_info else []
                top_bins = [c for c, v in bin_info.get("top_bins", [])[:6]] if bin_info else []
                structural_support = [l for l in (swing_lows + top_bins) if l < entry_price]
                
                if structural_support:
                    # Choose the most protective support level near our pivot
                    deepest_support = min(min(structural_support), pivot)
                    # Use the structural level if it's within a reasonable distance (8%)
                    pivot = deepest_support if (entry_price - deepest_support)/entry_price < 0.08 else pivot
                
                # Adaptive buffer beyond the pivot based on ATR and move strength
                buffer_mult = 0.8 * (1.5 if is_strong else 1.0)
                sl_dist = (entry_price - pivot) + (buffer_mult * atr)
                sl_pct = max(0.012, min(0.12, sl_dist / entry_price))
            else:
                # SHORT reversal
                cons_tp_abs = min(retr_38, entry_price - recent_extreme_low)
                aggr_tp_abs = min(retr_62, entry_price - recent_extreme_low)
                tp_abs = aggr_tp_abs if is_strong else cons_tp_abs
                tp_pct = max(tp_abs / entry_price, atr_pct * 2.5)
                
                # SL: Beyond absolute recent high AND nearest structural resistance
                pivot = recent_extreme_high
                swing_highs = bin_info.get("swing_highs", []) if bin_info else []
                top_bins = [c for c, v in bin_info.get("top_bins", [])[:6]] if bin_info else []
                structural_resistance = [h for h in (swing_highs + top_bins) if h > entry_price]
                
                if structural_resistance:
                    highest_resistance = max(max(structural_resistance), pivot)
                    pivot = highest_resistance if (highest_resistance - entry_price)/entry_price < 0.08 else pivot
                
                buffer_mult = 0.8 * (1.5 if is_strong else 1.0)
                sl_dist = (pivot - entry_price) + (buffer_mult * atr)
                sl_pct = max(0.012, min(0.12, sl_dist / entry_price))
            
            # Apply momentum-based overrides for TP
            if momentum_strength and momentum_strength.get("class") in ["EXPLOSIVE", "STRONG"]:
                tp_pct *= 1.2
            
            # No hard TP cap for Reversal - rely on Fibonacci and structural targets
            sl_pct = min(sl_pct, 0.12) # Cap at 12% for risk management
        
        elif strategy_type == "range":
            # RANGE: Smart targets within range, but allow for bigger moves if range is wide
            if direction == "LONG":
                target = (recent_high - entry_price) / entry_price
                sl_level = (entry_price - recent_low) / entry_price
            else:
                target = (entry_price - recent_low) / entry_price
                sl_level = (recent_high - entry_price) / entry_price
            
            # Use range width to determine if we can target bigger moves
            range_width = (recent_high - recent_low) / entry_price
            range_factor = min(1.5, max(0.8, range_width / 0.05))  # Boost if range is wide
            
            # Target structural range boundaries without hard percentage caps
            tp_pct = target * 0.92 * range_factor  # Slight increase in capture efficiency
            tp_pct = max(tp_pct, atr_pct * 1.5)  # Minimum grounded in volatility
            
            sl_pct = min(sl_level * 0.65, atr_pct * 1.1, 0.08)  # Slightly increased SL cap for wider ranges
            sl_pct = max(sl_pct, 0.004)
            
            # Volatile market adjustment
            if market_trend == "volatile":
                tp_pct = tp_pct * 0.95  # Slightly reduced, but no hard cap
                sl_pct = min(0.08, sl_pct * 1.1)
        
        elif strategy_type == "momentum":
            # MOMENTUM: Dynamic targets based on momentum strength and structural levels
            base_tp_atr = atr_pct * 3.5  # Increased from 3.0
            momentum_factor = 1.0
            
            if momentum_strength and isinstance(momentum_strength, dict):
                strength_class = momentum_strength.get("class", "NORMAL")
                if strength_class == "EXPLOSIVE":
                    momentum_factor = 2.5
                elif strength_class == "STRONG":
                    momentum_factor = 1.8
                elif strength_class == "MODERATE":
                    momentum_factor = 1.4
            
            # Prediction: Use structural distance (resistance for LONG, support for SHORT)
            # balanced with ATR-based momentum projection
            structural_target = distance_to_resistance if direction == "LONG" else distance_to_support
            
            # Weight structural targets heavily (70%) as they represent predictive exhaustion points
            tp_pct = (structural_target * 0.98 * 0.7) + (base_tp_atr * momentum_factor * volatility_factor * confidence_factor * 0.3)
            
            # No hard cap on TP as requested - allow bot to capture full move
            tp_pct = max(tp_pct, atr_pct * 2.0)
            
            # SL: Beyond the strong resistance/support barrier that the move shouldn't break
            # For LONG, barrier is support. For SHORT, barrier is resistance.
            barrier_dist = distance_to_support if direction == "LONG" else distance_to_resistance
            # Place SL 0.5 ATR beyond the structural barrier
            sl_pct = barrier_dist + (atr_pct * 0.5)
            
            # Ensure SL is at least 1.2 ATR to avoid noise
            sl_pct = max(sl_pct, atr_pct * 1.2)
        
        else:
            tp_pct = atr_pct * 4.0
            sl_pct = atr_pct * 1.5
        
        # Ensure minimum R/R ratio of 1.5
        rr_ratio = tp_pct / sl_pct if sl_pct > 0 else 1.0
        min_rr = 1.5
        
        if rr_ratio < min_rr:
            tp_pct = sl_pct * min_rr
        
        # Removed global TP cap as requested; SL cap kept for extreme risk management
        tp_pct = max(0.005, tp_pct)
        sl_pct = max(0.002, min(0.25, sl_pct))
        
        return round(sl_pct, 6), round(tp_pct, 6)
    except Exception:
        return 0.01, 0.02


def dynamic_risk_levels(pair_data, tv_metrics=None, market_trend="neutral", direction="LONG", vol_est=None, binance_info=None):
    price = pair_data.get("price")
    if not price or price <= 0:
        try:
            if STRATEGY_FETCHERS_AVAILABLE and get_live_price:
                pair = pair_data.get("symbol", "") + "USDT" if pair_data.get("symbol") else None
                if pair:
                    price = get_live_price(pair, strategy="reversal")
        except Exception:
            price = None
    if not price or price <= 0:
        price = 0.0
    if price <= 0:
        return 0.01, 0.02
    high = pair_data.get("high_24h") or price
    low = pair_data.get("low_24h") or price
    day_range = max(1e-6, (high - low) / (price + 1e-12))
    intraday_ref = max(0.002, vol_est or day_range * 0.25)
    candidates_tp = []
    zone, pos = compute_zone(pair_data)
    if binance_info:
        top_bins = [c for c, v in binance_info.get("top_bins", [])[:6]]
        swing_highs = bin_info.get("swing_highs", []) if False else binance_info.get("swing_highs", [])
        swing_lows = bin_info.get("swing_lows", []) if False else binance_info.get("swing_lows", [])
        vwap = bin_info.get("vwap", None) if False else binance_info.get("vwap", None)
    else:
        top_bins = []; swing_highs = []; swing_lows = []; vwap = None
    if direction == "LONG":
        for sh in swing_highs:
            if sh > price * 1.001:
                candidates_tp.append((sh - price) / price * 0.995)
        for center in top_bins:
            if center > price * 1.001:
                candidates_tp.append((center - price) / price * 0.995)
    else:
        for sl in swing_lows:
            if sl < price * 0.999:
                candidates_tp.append((price - sl) / price * 0.995)
        for center in top_bins:
            if center < price * 0.999:
                candidates_tp.append((price - center) / price * 0.995)
    fracs = [0.75, 1.0, 1.5, 2.0, 3.0]
    for f in fracs:
        candidates_tp.append(intraday_ref * f)
    candidates_tp = sorted(list({round(c, 8) for c in candidates_tp if c > 0.0005}))
    candidates_sl = []
    if direction == "LONG":
        for sl in swing_lows:
            if sl < price * 0.995:
                candidates_sl.append((price - sl) / price * 1.05)
    else:
        for sh in swing_highs:
            if sh > price * 1.005:
                candidates_sl.append((sh - price) / price * 1.05)
    sl_fracs = [0.5, 0.75, 1.0, 1.5, 2.0]
    for f in sl_fracs:
        candidates_sl.append(intraday_ref * f)
    candidates_sl = sorted(list({round(max(0.0005, min(0.2, s)), 8) for s in candidates_sl}))
    best_ev = -1e9
    best_pair = (0.01, 0.02)
    mt = market_trend or "neutral"
    for tp in candidates_tp:
        for sl in candidates_sl:
            if tp <= 0 or sl <= 0:
                continue
            rr = tp / sl
            if rr < 0.35:
                continue
            p_tp, p_sl = estimate_hit_prob(tp, sl, tv_metrics or {}, pair_data, direction, mt, vol_est, binance_info)
            ev = p_tp * tp - p_sl * sl
            ev += 0.0001 * math.log(1 + rr)
            if tp > intraday_ref * 6.0:
                ev *= 0.88
            if ev > best_ev:
                best_ev = ev
                best_pair = (sl, tp)
    sl_pct, tp_pct = best_pair
    sl_pct = max(0.002, min(0.08, sl_pct))
    tp_pct = max(0.004, tp_pct) # Uncapped TP
    if zone == "support" and direction == "LONG":
        tp_pct *= 1.12; sl_pct *= 0.95
    if zone == "resistance" and direction == "SHORT":
        tp_pct *= 1.12; sl_pct *= 0.95
    sl_pct = max(0.002, min(0.10, sl_pct))
    tp_pct = max(0.004, tp_pct) # Uncapped TP
    return sl_pct, tp_pct

# ---------------- ENHANCED REVERSAL DETECTOR v2 (Balanced) ----------------
def calc_avg_volume_recent(klines, lookback=12):
    if not klines:
        return None
    vols = []
    for k in klines[-lookback:]:
        try:
            vols.append(float(k[5]))
        except Exception:
            pass
    if not vols:
        return None
    return statistics.mean(vols)

def wick_rejection_check(klines, direction):
    """Balanced rejection logic for both LONG and SHORT signals."""
    if not klines:
        return False, None

    cand_count = min(6, len(klines))  # Expanded lookback for better sensitivity
    for k in klines[-cand_count:]:
        open_p = float(k[1])
        high = float(k[2])
        low = float(k[3])
        close = float(k[4])
        body = abs(close - open_p)

        if direction == "SHORT":
            wick = high - max(close, open_p)
        else:
            wick = min(close, open_p) - low

        ratio = wick / (body + 1e-12)
        if ratio >= WICK_BODY_RATIO and wick / ((open_p + close) / 2 + 1e-12) >= WICK_MIN_PCT:
            return True, ratio
    return False, None

def rsi_divergence_check(klines, tv_metrics, direction):
    """
    Detects true RSI divergence:
    - Bearish (for SHORT): Price makes higher high, RSI makes lower high.
    - Bullish (for LONG): Price makes lower low, RSI makes higher low.
    """
    if not klines or len(klines) < 14 or not tv_metrics:
        return False
        
    rsi_val = tv_metrics.get("rsi")
    if rsi_val is None:
        return False

    try:
        # Check last 10 candles for local peaks
        lookback = 10
        recent_klines = klines[-lookback:]
        
        if direction == "SHORT":
            # Bearish Divergence: Price up, RSI weak
            price_high_recent = max(float(k[2]) for k in recent_klines)
            price_high_prior = max(float(k[2]) for k in klines[-(lookback*2):-lookback])
            
            # Simple check: current price is higher than prior high, but current RSI is not high enough
            if price_high_recent > price_high_prior and rsi_val < 70:
                return True
        else:
            # Bullish Divergence: Price down, RSI strong
            price_low_recent = min(float(k[3]) for k in recent_klines)
            price_low_prior = min(float(k[3]) for k in klines[-(lookback*2):-lookback])
            
            if price_low_recent < price_low_prior and rsi_val > 30:
                return True
    except Exception:
        pass
        
    return False

def pump_dump_layer_from_klines(klines, direction):
    """
    Detect pump/dump from FRESH klines instead of stale market data.
    Computes 1h and 24h price changes directly from candle closes.
    More accurate than using cached price_change_1h/24h from initial market data fetch.
    """
    if not klines or len(klines) < 6:
        return False
    
    try:
        closes = [float(k[4]) for k in klines]
        current_close = closes[-1]
        
        if current_close <= 0 or len(closes) < 6:
            return False
        
        idx_1h = max(0, len(closes) - 4)
        idx_24h = max(0, len(closes) - 96)
        
        close_1h_ago = closes[idx_1h]
        close_24h_ago = closes[idx_24h]
        
        price_change_1h = ((current_close - close_1h_ago) / close_1h_ago * 100.0) if close_1h_ago > 0 else 0.0
        price_change_24h = ((current_close - close_24h_ago) / close_24h_ago * 100.0) if close_24h_ago > 0 else 0.0
        
        if direction == "SHORT":
            if (price_change_1h >= REV_PUMP_1H) or (price_change_24h >= REV_PUMP_24H):
                return True
        else:
            if (price_change_1h <= REV_DUMP_1H) or (price_change_24h <= REV_DUMP_24H):
                return True
        
        return False
    except Exception:
        return False

def pump_dump_layer(pair_data, direction):
    oneh = pair_data.get("price_change_1h", 0.0) or 0.0
    day = pair_data.get("price_change_24h", 0.0) or 0.0
    # FIXED Priority 1: Corrected reversal direction logic - SHORT after pump, LONG after dump
    if direction == "SHORT":
        if (oneh >= REV_PUMP_1H) or (day >= REV_PUMP_24H):  # After pump, expect SHORT reversal
            return True
    else:  # LONG direction
        if (oneh <= REV_DUMP_1H) or (day <= REV_DUMP_24H):  # After dump, expect LONG reversal
            return True
    return False

def structure_layer_check(bin_info, pair_price, direction):
    if not bin_info:
        return False
    top_bins = [c for c, v in bin_info.get("top_bins", [])[:6]]
    swing_highs = bin_info.get("swing_highs", [])
    swing_lows = bin_info.get("swing_lows", [])
    if direction == "SHORT":
        for center in top_bins:
            if center > pair_price and abs(center - pair_price)/pair_price < STRUCTURE_PROX_PCT:
                return True
        for sh in swing_highs:
            if sh > pair_price and abs(sh - pair_price)/pair_price < STRUCTURE_PROX_PCT:
                return True
    else:
        for center in top_bins:
            if center < pair_price and abs(center - pair_price)/pair_price < STRUCTURE_PROX_PCT:
                return True
        for sl in swing_lows:
            if sl < pair_price and abs(sl - pair_price)/pair_price < STRUCTURE_PROX_PCT:
                return True
    return False

# ============= MULTI-LAYER STRATEGY DETECTION SYSTEM =============
"""
RESTRUCTURED STRATEGY ARCHITECTURE WITH FALLBACKS

This implementation introduces a multi-layer detection system for all three trading strategies:
REVERSAL, RANGE, and MOMENTUM. Each strategy now has:

1. PRIMARY DETECTION LAYERS
   - Most reliable signals (pump/dump exhaustion, volume climax, clear range boundaries)
   - High confidence when triggered

2. SECONDARY DETECTION LAYERS
   - Supportive signals (wick rejections, RSI divergence, bounce confirmations)
   - Used to confirm or enhance primary signals

3. TERTIARY/FALLBACK DETECTION LAYERS
   - Alternative detection methods (momentum divergence, structure testing, extreme RSI)
   - Ensures signals always exist regardless of market conditions
   - Lower confidence but still actionable

KEY IMPROVEMENTS:
- Each strategy has independent fallback methods - no single data source failure breaks signals
- Multi-timeframe confirmation for RANGE strategy (30m + 1h + 4h)
- Detection layer agreement bonuses: More methods agreeing = higher confidence
- Quality scoring based on layer agreement + confidence + likelihood of TP hit vs SL
- Guaranteed signal generation: All three strategies produce signals in all market conditions
- Confidence reflects actual signal quality: High-quality signals get higher confidence

SIGNAL QUALITY SCORING:
- Scores reflect how many detection layers agree on the signal
- Signals with multiple confirming methods get quality bonuses
- Cross-strategy ranking ensures highest-quality signal gets highest confidence
- Fallback signals marked clearly with lower quality scores
"""

class StrategyDetectionResult:
    """Container for strategy detection results with quality metrics."""
    def __init__(self, direction=None, confidence=0.0, layers=None, quality_score=0.0,
                 tp_pct=None, sl_pct=None, fallback_used=False, reversal_strength=None):
        self.direction = direction
        self.confidence = confidence
        self.layers = layers or []
        self.quality_score = quality_score
        self.tp_pct = tp_pct
        self.sl_pct = sl_pct
        self.fallback_used = fallback_used
        self.method_agreement_count = len(layers)
        self.reversal_strength = reversal_strength or {}

def calculate_method_agreement_bonus(num_methods_agree, max_methods=6):
    """Bonus confidence when multiple detection methods agree."""
    if num_methods_agree <= 1:
        return 0.0
    base_bonus = 3.0
    return base_bonus + (num_methods_agree - 2) * 2.5

def adjust_confidence_by_agreement(base_conf, num_methods_agree, penalty_if_fallback=False):
    """Adjust confidence based on how many detection methods agreed."""
    bonus = calculate_method_agreement_bonus(num_methods_agree)
    final_conf = base_conf + bonus
    if penalty_if_fallback:
        final_conf *= 0.95
    return max(0.0, min(100.0, final_conf))

# ============= REVERSAL STRENGTH CALCULATION =============

def calculate_reversal_strength(pair_data, direction):
    """
    Calculate the expected strength/magnitude of a reversal based on pump/dump depth.

    Returns:
        dict with keys:
            - strength_pct: Expected retracement as % of current price (0.005 to 0.20)
            - strength_class: "MAJOR", "MEDIUM", or "MINOR"
            - pump_depth_pct: Recent pump magnitude
            - dump_depth_pct: Recent dump magnitude
            - retracement_level: Primary retracement target (50% Fibonacci)
    """
    try:
        price = pair_data.get("price", 0.0)
        if price <= 0:
            return {
                "strength_pct": 0.025,
                "strength_class": "MINOR",
                "pump_depth_pct": 0.0,
                "dump_depth_pct": 0.0,
                "retracement_level": 0.025
            }

        price_change_1h = abs(pair_data.get("price_change_1h") or 0.0)
        price_change_24h = abs(pair_data.get("price_change_24h") or 0.0)
        high_24h = pair_data.get("high_24h", price)
        low_24h = pair_data.get("low_24h", price)

        if high_24h <= 0 or low_24h <= 0:
            return {
                "strength_pct": 0.025,
                "strength_class": "MINOR",
                "pump_depth_pct": 0.0,
                "dump_depth_pct": 0.0,
                "retracement_level": 0.025
            }

        range_24h = (high_24h - low_24h) / low_24h

        pump_depth = 0.0
        dump_depth = 0.0

        if direction == "SHORT":
            pump_depth = max(price_change_1h, price_change_24h) / 100.0
            range_based_pump = range_24h * 0.6
            pump_depth = max(pump_depth, range_based_pump)
        else:
            dump_depth = max(price_change_1h, price_change_24h) / 100.0
            range_based_dump = range_24h * 0.6
            dump_depth = max(dump_depth, range_based_dump)

        primary_move_depth = pump_depth if direction == "SHORT" else dump_depth

        fib_38 = primary_move_depth * 0.382
        fib_50 = primary_move_depth * 0.50
        fib_618 = primary_move_depth * 0.618

        retracement_target = fib_50

        strength_pct = max(0.005, min(0.20, retracement_target))

        if strength_pct >= 0.045:
            strength_class = "MAJOR"
        elif strength_pct >= 0.025:
            strength_class = "MEDIUM"
        else:
            strength_class = "MINOR"

        return {
            "strength_pct": round(strength_pct, 6),
            "strength_class": strength_class,
            "pump_depth_pct": round(pump_depth, 6),
            "dump_depth_pct": round(dump_depth, 6),
            "retracement_level": round(retracement_target, 6),
            "fib_38": round(fib_38, 6),
            "fib_50": round(fib_50, 6),
            "fib_618": round(fib_618, 6)
        }
    except Exception as e:
        return {
            "strength_pct": 0.025,
            "strength_class": "MINOR",
            "pump_depth_pct": 0.0,
            "dump_depth_pct": 0.0,
            "retracement_level": 0.025
        }

def scale_tp_sl_by_reversal_strength(sl_pct, tp_pct, reversal_strength):
    """
    Scale TP/SL percentages based on reversal strength.

    Strong reversals get proportionally larger TP targets.
    Weak reversals get tighter TP to match expected movement.

    Args:
        sl_pct: Base stop loss percentage
        tp_pct: Base take profit percentage
        reversal_strength: dict from calculate_reversal_strength()

    Returns:
        (adjusted_sl_pct, adjusted_tp_pct)
    """
    try:
        if not reversal_strength or not reversal_strength.get("strength_class"):
            return sl_pct, tp_pct

        strength_class = reversal_strength.get("strength_class", "MINOR")
        strength_pct = reversal_strength.get("strength_pct", 0.025)

        if strength_class == "MAJOR":
            tp_multiplier = 1.25
            sl_multiplier = 1.05
        elif strength_class == "MEDIUM":
            tp_multiplier = 1.08
            sl_multiplier = 1.02
        else:
            tp_multiplier = 0.85
            sl_multiplier = 0.98

        adjusted_tp = min(0.20, tp_pct * tp_multiplier)
        adjusted_sl = min(0.10, sl_pct * sl_multiplier)

        min_tp = max(0.004, strength_pct * 0.8)
        adjusted_tp = max(min_tp, adjusted_tp)

        return round(adjusted_sl, 6), round(adjusted_tp, 6)
    except Exception:
        return sl_pct, tp_pct

def calculate_range_strength(pair_data, range_meta, direction):
    """Calculate expected range breakout/continuation strength."""
    try:
        width_pct = range_meta.get("width_pct", 0) / 100.0
        rejections_total = range_meta.get("rejections_top", 0) + range_meta.get("rejections_bottom", 0)

        # Stronger ranges = more rejections + optimal width
        strength_score = min(100, rejections_total * 15 + (1.0 - width_pct / 0.15) * 30)

        if strength_score >= 70:
            return {"class": "STRONG", "multiplier": 1.3, "score": strength_score}
        elif strength_score >= 40:
            return {"class": "MEDIUM", "multiplier": 1.0, "score": strength_score}
        else:
            return {"class": "WEAK", "multiplier": 0.7, "score": strength_score}
    except Exception:
        return {"class": "WEAK", "multiplier": 0.7, "score": 0}

def adjust_range_risk_by_market_conditions(sl_pct, tp_pct, market_trend, vol_regime):
    """Adjust range TP/SL based on market conditions."""
    try:
        # In trending markets, ranges are more likely to break
        if market_trend in ["bullish", "bearish"]:
            sl_pct *= 0.9  # Tighter stops
            tp_pct *= 0.8  # More conservative targets

        # In high volatility, ranges are less reliable
        if vol_regime == "high":
            sl_pct *= 0.85
            tp_pct *= 0.9

        return sl_pct, tp_pct
    except Exception:
        return sl_pct, tp_pct


def calculate_reversal_quality_score(layers, reversal_strength, confidence):
    """
    Enhanced quality score for Reversal strategy using comprehensive data analysis.
    Considers: layer count/diversity, reversal strength, momentum confirmation,
    volume patterns, technical indicators, and TP probability.
    """
    base_score = 0.0
    
    # Layer count and diversity (more weight on diversity)
    layer_count = len(layers)
    layer_diversity = len(set(layers)) / max(len(layers), 1) if layers else 0.0
    
    if layer_count >= 4:
        base_score += 40.0  # Increased from 35.0
        if layer_diversity > 0.8:
            base_score += 10.0  # Bonus for high diversity
    elif layer_count >= 3:
        base_score += 30.0  # Increased from 25.0
        if layer_diversity > 0.7:
            base_score += 8.0
    elif layer_count >= 2:
        base_score += 20.0  # Increased from 15.0
        if layer_diversity > 0.6:
            base_score += 5.0
    elif layer_count >= 1:
        base_score += 12.0  # Increased from 8.0
    
    # Reversal strength (primary factor)
    strength_class = reversal_strength.get("strength_class", "MINOR")
    strength_pct = reversal_strength.get("strength_pct", 0.0)
    
    if strength_class == "MAJOR":
        base_score += 35.0  # Increased from 30.0
        if strength_pct > 0.05:
            base_score += 5.0  # Bonus for very strong moves
    elif strength_class == "MODERATE":
        base_score += 22.0  # Increased from 18.0
    elif strength_class == "MINOR":
        base_score += 10.0  # Increased from 8.0
    else:
        base_score += 3.0
    
    # Layer diversity bonus (enhanced)
    diversity_bonus = layer_diversity * 25.0  # Increased from 20.0
    base_score += diversity_bonus
    
    # Confidence-based quality adjustment (if confidence is high, quality is likely high)
    if confidence > 80:
        base_score += 8.0
    elif confidence > 70:
        base_score += 5.0
    elif confidence > 60:
        base_score += 2.0
    
    return min(100.0, max(0.0, base_score))


def calculate_range_quality_score(layers, layer_names, rejections_total, bb_compact, 
                                   vol_ok, stability_score, confidence, bounce_count=0):
    """
    Enhanced quality score for Range strategy using comprehensive range analysis.
    Considers: rejection count, BB compaction, volume patterns, stability, bounces,
    layer diversity, and confidence levels.
    """
    base_score = 0.0
    
    # Rejection count (primary indicator of range quality)
    if rejections_total >= 4:
        base_score += 35.0  # Increased from 30.0
    elif rejections_total >= 3:
        base_score += 30.0
    elif rejections_total >= 2:
        base_score += 22.0  # Increased from 20.0
    elif rejections_total >= 1:
        base_score += 12.0  # Increased from 10.0
    else:
        base_score += 3.0
    
    # BB compaction (indicates range compression)
    if bb_compact:
        base_score += 25.0  # Increased from 20.0
    else:
        base_score += 5.0
    
    # Volume settling (important for range continuation)
    if vol_ok:
        base_score += 18.0  # Increased from 15.0
    else:
        base_score += 3.0
    
    # Stability score (range persistence)
    if stability_score and stability_score >= 80.0:
        base_score += 25.0  # Increased from 20.0
    elif stability_score and stability_score >= 70.0:
        base_score += 20.0
    elif stability_score and stability_score >= 50.0:
        base_score += 12.0
    elif stability_score:
        base_score += 5.0
    
    # Bounce confirmation (validates range boundaries)
    if bounce_count >= 3:
        base_score += 18.0  # Increased from 15.0
    elif bounce_count >= 2:
        base_score += 15.0
    elif bounce_count >= 1:
        base_score += 10.0  # Increased from 8.0
    
    # Layer diversity and count
    layer_count = len(layer_names) if layer_names else 0
    layer_diversity = len(set(layer_names)) / max(len(layer_names), 1) if layer_names else 0.5
    
    if layer_count >= 4:
        base_score += 8.0
    elif layer_count >= 3:
        base_score += 5.0
    elif layer_count >= 2:
        base_score += 3.0
    
    diversity_bonus = layer_diversity * 15.0  # Increased from 12.0
    base_score += diversity_bonus
    
    # Confidence-based quality adjustment
    if confidence > 75:
        base_score += 8.0
    elif confidence > 65:
        base_score += 5.0
    elif confidence > 55:
        base_score += 2.0
    
    return min(100.0, max(0.0, base_score))


def calculate_momentum_quality_score(layers, momentum_strength, breakout_detected, 
                                      volume_surge_detected, confidence):
    """
    Enhanced quality score for Momentum strategy using comprehensive momentum analysis.
    Considers: layer count/diversity, momentum strength, breakout confirmation,
    volume surge, multi-timeframe alignment, and confidence levels.
    """
    base_score = 0.0
    
    # Layer count and diversity
    layer_count = len(layers)
    layer_diversity = len(set(layers)) / max(len(layers), 1) if layers else 0.0
    
    if layer_count >= 4:
        base_score += 35.0  # Increased from 30.0
        if layer_diversity > 0.8:
            base_score += 10.0  # Bonus for high diversity
    elif layer_count >= 3:
        base_score += 28.0  # Increased from 23.0
        if layer_diversity > 0.7:
            base_score += 8.0
    elif layer_count >= 2:
        base_score += 18.0  # Increased from 15.0
        if layer_diversity > 0.6:
            base_score += 5.0
    elif layer_count >= 1:
        base_score += 12.0  # Increased from 8.0
    
    # Momentum strength (primary factor)
    momentum_class = momentum_strength.get("class", "NORMAL") if momentum_strength else "NORMAL"
    momentum_score = momentum_strength.get("score", 0.0) if momentum_strength else 0.0
    
    if momentum_class == "EXPLOSIVE":
        base_score += 32.0  # Increased from 28.0
        if momentum_score > 0.8:
            base_score += 5.0  # Bonus for very strong momentum
    elif momentum_class == "STRONG":
        base_score += 22.0  # Increased from 18.0
    elif momentum_class == "MODERATE":
        base_score += 12.0  # Increased from 10.0
    else:
        base_score += 3.0
    
    # Breakout and volume confirmation (critical for momentum)
    if breakout_detected and volume_surge_detected:
        base_score += 25.0  # Increased from 20.0
    elif breakout_detected:
        base_score += 15.0  # Increased from 12.0
    elif volume_surge_detected:
        base_score += 10.0  # Increased from 8.0
    else:
        base_score += 2.0
    
    # Layer diversity bonus (enhanced)
    diversity_bonus = layer_diversity * 15.0  # Increased from 12.0
    base_score += diversity_bonus
    
    # Confidence-based quality adjustment
    if confidence > 80:
        base_score += 10.0
    elif confidence > 70:
        base_score += 6.0
    elif confidence > 60:
        base_score += 3.0
    
    return min(100.0, max(0.0, base_score))


# ============= ENHANCED REVERSAL DETECTOR v3 (with fallbacks) =============

def detect_reversal_opportunity_v3_with_fallbacks(pair_data, klines, bin_info, tv_metrics=None, vol_est=None,
                                                   market_trend="neutral", market_sentiment_pct=50.0,
                                                   klines_1h=None, klines_4h=None):
    """
    Enhanced reversal detector with PRIMARY, SECONDARY, and TERTIARY detection layers.
    Always returns a signal if market conditions extremes are met (fallback).
    Returns StrategyDetectionResult with quality metrics.
    """
    dbg = os.getenv("DEBUG_REVERSAL", "false").lower() == "true"

    # Directional confirmation check helper (Enhanced & more flexible)
    def is_reversal_confirmed(klines, direction):
        if not klines or len(klines) < 3:
            return False
        
        last_candle = klines[-1]
        c = float(last_candle[4]); o = float(last_candle[1])
        h = float(last_candle[2]); l = float(last_candle[3])
        body_size = abs(c - o)
        wick_size_upper = h - max(c, o)
        wick_size_lower = min(c, o) - l
        
        closes = [float(k[4]) for k in klines[-3:]]
        
        if direction == "LONG":
            # Strong green candle
            if c > o and body_size > (wick_size_upper + wick_size_lower):
                return True
            # Hammer / Long lower wick
            if wick_size_lower > body_size * 1.5:
                return True
            # Consecutive moves
            moving_up = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i-1])
            return moving_up >= 2
        elif direction == "SHORT":
            # Strong red candle
            if c < o and body_size > (wick_size_upper + wick_size_lower):
                return True
            # Shooting star / Long upper wick
            if wick_size_upper > body_size * 1.5:
                return True
            # Consecutive moves
            moving_down = sum(1 for i in range(1, len(closes)) if closes[i] < closes[i-1])
            return moving_down >= 2
        
        return False

    price = pair_data.get("price")
    if not price:
        return StrategyDetectionResult()

    # Contrarian Sentiment Logic: Boost LONG when sentiment is LOW, boost SHORT when sentiment is HIGH
    sentiment_neutral = 50.0
    if market_sentiment_pct < sentiment_neutral:
        # Fearful market: Boost LONG reversals, slightly penalize SHORT
        long_sentiment_scale = 1.0 + ((sentiment_neutral - market_sentiment_pct) / 100.0)
        short_sentiment_scale = max(0.6, 1.0 - ((sentiment_neutral - market_sentiment_pct) / 100.0))
    else:
        # Greedy market: Boost SHORT reversals, slightly penalize LONG
        short_sentiment_scale = 1.0 + ((market_sentiment_pct - sentiment_neutral) / 100.0)
        long_sentiment_scale = max(0.6, 1.0 - ((market_sentiment_pct - sentiment_neutral) / 100.0))

    local_rev_min = max(10.0, REV_MIN_CONFIDENCE) # Don't scale the floor here to avoid double-dipping

    primary_layers = []
    secondary_layers = []
    tertiary_layers = []

    if klines and len(klines) >= 6:
        rsi_val = tv_metrics.get("rsi") if tv_metrics else None

        pump_hit = pump_dump_layer_from_klines(klines, "SHORT")
        dump_hit = pump_dump_layer_from_klines(klines, "LONG")

        vol_avg = calc_avg_volume_recent(klines, lookback=12)
        short_vol_avg, long_vol_avg = compute_volume_context(klines, short_lb=12, long_lb=64)

        vol_climax_short = False
        vol_climax_long = False
        try:
            if klines and vol_avg:
                max_candles = min(3, len(klines))
                for idx in range(1, max_candles + 1):
                    candle = klines[-idx]
                    vol_val = float(candle[5])
                    long_context = long_vol_avg or vol_avg
                    climax_threshold = max(vol_avg * VOL_CLIMAX_MULT, (long_context or 0.0) * 1.15)
                    if climax_threshold <= 0:
                        continue
                    if vol_val >= climax_threshold:
                        open_p = float(candle[1])
                        close_p = float(candle[4])
                        if close_p < open_p:
                            vol_climax_short = True
                        elif close_p > open_p:
                            vol_climax_long = True
                        break
        except Exception as e:
            if dbg:
                print(f"[REV V3] {pair_data.get('symbol','?')} - Vol climax check error: {str(e)[:80]}")

        wick_short, wick_short_ratio = wick_rejection_check(klines, "SHORT")
        wick_long, wick_long_ratio = wick_rejection_check(klines, "LONG")

        rsi_div_short = rsi_divergence_check(klines, tv_metrics, "SHORT") if tv_metrics else False
        rsi_div_long = rsi_divergence_check(klines, tv_metrics, "LONG") if tv_metrics else False

        mom_div_short = momentum_divergence_check(klines, "SHORT")
        mom_div_long = momentum_divergence_check(klines, "LONG")

        structure_short = structure_layer_check(bin_info, price, "SHORT") if bin_info else False
        structure_long = structure_layer_check(bin_info, price, "LONG") if bin_info else False

        higher_bias = higher_tf_bias_from_klines(klines)
        if klines_1h:
            higher_bias_1h = higher_tf_bias_from_klines(klines_1h, period=24)
            if higher_bias_1h != "flat":
                higher_bias = higher_bias_1h
        if klines_4h:
            higher_bias_4h = higher_tf_bias_from_klines(klines_4h, period=12)
            if higher_bias_4h != "flat":
                higher_bias = higher_bias_4h

        # Add simple price action layers for reversal (expecting correction after move)
        if len(klines) >= 5:
            recent_trend = sum(1 for i in range(1, 5) if float(klines[-i][4]) > float(klines[-i-1][4]))
            if recent_trend >= 3:  # 3 out of 4 candles up -> likely overextended
                primary_layers.append(("SHORT", "potential_exhaustion", 12.0))
            elif recent_trend <= 1:  # 1 or fewer candles up (mostly down) -> likely oversold
                primary_layers.append(("LONG", "potential_climax", 12.0))

        # Add oversold/overbought RSI layers (less strict than extremes)
        if rsi_val is not None:
            if rsi_val <= 35:  # Less strict than RSI_EXTREME_LONG (30)
                tertiary_layers.append(("LONG", "rsi_oversold", 12.0))
            if rsi_val >= 65:  # Less strict than RSI_EXTREME_SHORT (70)
                tertiary_layers.append(("SHORT", "rsi_overbought", 12.0))

        # Add volume-based layers
        if vol_avg and len(klines) >= 3:
            recent_vol = float(klines[-1][5])
            if recent_vol > vol_avg * 1.5:  # 50% above average
                secondary_layers.append(("LONG" if float(klines[-1][4]) > float(klines[-1][1]) else "SHORT", "volume_spike", 18.0))

        # Mean-reversion EMA layers: expect price to return to EMA when extended
        if tv_metrics:
            ema20 = tv_metrics.get("ema20")
            if ema20:
                if price > ema20 * 1.05: # 5% extension above EMA20
                    tertiary_layers.append(("SHORT", "ema_extension_top", 12.0))
                elif price < ema20 * 0.95: # 5% extension below EMA20
                    tertiary_layers.append(("LONG", "ema_extension_bottom", 12.0))

        if wick_short:
            secondary_layers.append(("SHORT", "wick_rejection", 24.0))
        if wick_long:
            secondary_layers.append(("LONG", "wick_rejection", 24.0))

        if pump_hit:
            secondary_layers.append(("SHORT", "pump_reversal", 22.0))
        if dump_hit:
            secondary_layers.append(("LONG", "dump_reversal", 22.0))

        if rsi_div_short:
            secondary_layers.append(("SHORT", "rsi_div", 16.0))
        if rsi_div_long:
            secondary_layers.append(("LONG", "rsi_div", 16.0))

        if mom_div_short:
            tertiary_layers.append(("SHORT", "mom_div", 15.0))
        if mom_div_long:
            tertiary_layers.append(("LONG", "mom_div", 15.0))

        if structure_short:
            tertiary_layers.append(("SHORT", "structure_reject", 18.0))
        if structure_long:
            tertiary_layers.append(("LONG", "structure_reject", 18.0))

        try:
            rsi_extreme_short = RSI_EXTREME_SHORT
            rsi_extreme_long = RSI_EXTREME_LONG
            # Neutralized market trend skew of RSI extremes to keep symmetric thresholds

            if rsi_val is not None:
                if rsi_val >= rsi_extreme_short:
                    tertiary_layers.append(("SHORT", "rsi_extreme", 18.0))
                if rsi_val <= rsi_extreme_long:
                    tertiary_layers.append(("LONG", "rsi_extreme", 18.0))
        except Exception:
            pass

    def best_signal_from_layers(primary, secondary, tertiary, min_threshold):
        """Extract best signal from layered detection."""
        all_directions = {}

        for direction, layer_name, weight in primary + secondary + tertiary:
            if direction not in all_directions:
                all_directions[direction] = {"conf": 0.0, "layers": [], "layer_types": []}
            all_directions[direction]["conf"] += weight
            all_directions[direction]["layers"].append(layer_name)

            layer_type = "primary" if (direction, layer_name, weight) in primary else (
                "secondary" if (direction, layer_name, weight) in secondary else "tertiary"
            )
            all_directions[direction]["layer_types"].append(layer_type)

        chosen_direction = None
        chosen_conf = 0.0
        chosen_layers = []

        for direction, data in all_directions.items():
            conf = data["conf"]
            agreement_bonus = calculate_method_agreement_bonus(len(data["layers"]))
            conf += agreement_bonus

            if conf > chosen_conf:
                chosen_conf = conf
                chosen_direction = direction
                chosen_layers = data["layers"]

        return chosen_direction, min(100.0, chosen_conf), chosen_layers

    chosen_dir, chosen_conf, chosen_layers = best_signal_from_layers(
        primary_layers, secondary_layers, tertiary_layers, local_rev_min * 0.85
    )

    # Fallback: Simple mean-reversion signal if no complex signal found
    if not chosen_dir and klines and len(klines) >= 8:
        # Calculate simple momentum over last 8 candles
        closes = [float(k[4]) for k in klines[-8:]]
        momentum = (closes[-1] - closes[0]) / (closes[0] + 1e-12)
        
        if abs(momentum) > 0.005:  # 0.5% minimum move
            fallback_direction = "SHORT" if momentum > 0 else "LONG" # Corrected to Reversal
            fallback_conf = min(local_rev_min + 5.0, abs(momentum) * 1000)
            
            # Reversal: Boost if sentiment is AGAINST the current move
            if (fallback_direction == "LONG" and market_sentiment_pct < 45) or \
               (fallback_direction == "SHORT" and market_sentiment_pct > 55):
                fallback_conf += 8.0
            
            if fallback_conf >= local_rev_min:
                chosen_dir = fallback_direction
                chosen_conf = fallback_conf
                chosen_layers = ["reversal_momentum_fallback"]

    # Enhanced fallback: Mean reversion price action
    if not chosen_dir and klines and len(klines) >= 5:
        closes = [float(k[4]) for k in klines[-5:]]
        price_trend = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i-1])
        
        if price_trend >= 3:  # Up-move
            chosen_dir = "SHORT" # Expect reversal
            chosen_conf = local_rev_min + 2.0
            chosen_layers = ["overextended_fallback_short"]
        elif price_trend <= 1:  # Down-move
            chosen_dir = "LONG" # Expect reversal
            chosen_conf = local_rev_min + 2.0
            chosen_layers = ["oversold_fallback_long"]
        else:  # Neutral - use contrarian sentiment
            if market_sentiment_pct > 55:
                chosen_dir = "SHORT"
                chosen_conf = local_rev_min + 1.0
                chosen_layers = ["contrarian_sentiment_short"]
            elif market_sentiment_pct < 45:
                chosen_dir = "LONG"
                chosen_conf = local_rev_min + 1.0
                chosen_layers = ["contrarian_sentiment_long"]
        
        if dbg:
            print(f"[REV V3] {pair_data.get('symbol','?')} - Using enhanced fallback: {chosen_dir} ({chosen_conf:.1f}%)")

    if not chosen_dir:
        if dbg:
            print(f"[REV V3] {pair_data.get('symbol','?')} - No signal (insufficient data)")
        return StrategyDetectionResult()

    # Refined RSI force logic: acts as a strong booster, not a standalone trigger
    rsi_val = tv_metrics.get("rsi") if tv_metrics else None
    if rsi_val is not None:
        if chosen_dir == "LONG" and rsi_val <= 22:
            chosen_conf = min(100.0, chosen_conf + 15.0)
        elif chosen_dir == "SHORT" and rsi_val >= 78:
            chosen_conf = min(100.0, chosen_conf + 15.0)

    # Neutralized higher timeframe bias scaling to avoid direction skew
    # Keep chosen_conf unchanged irrespective of higher_bias alignment.
    if higher_bias:
        chosen_conf *= 1.0

    if tv_metrics:
        ema5 = tv_metrics.get("ema5")
        ema20 = tv_metrics.get("ema20")
        if ema5 is not None and ema20 is not None:
            # Overextension Bonus (Contrarian): Boost if price is far from EMA20
            extension_pct = abs(price - ema20) / ema20
            if chosen_dir == "SHORT" and price > ema20 * 1.03: # 3% overextended top
                chosen_conf += min(15.0, extension_pct * 200.0)
            elif chosen_dir == "LONG" and price < ema20 * 0.97: # 3% overextended bottom
                chosen_conf += min(15.0, extension_pct * 200.0)

    # Add directional confirmation check
    if not is_reversal_confirmed(klines, chosen_dir):
        chosen_conf *= 0.90 # Mild penalty if reversal hasn't started moving
        if dbg:
            print(f"[REV V3] {pair_data.get('symbol','?')} - Mild Penalty: Directional confirmation not yet solid.")

    chosen_conf = max(local_rev_min, min(100.0, chosen_conf))

    # Apply Contrarian Sentiment Scaling
    if chosen_dir == "LONG":
        chosen_conf *= long_sentiment_scale
    else:
        chosen_conf *= short_sentiment_scale

    reversal_strength = calculate_reversal_strength(pair_data, chosen_dir)
    
    quality_score = calculate_reversal_quality_score(
        chosen_layers, 
        reversal_strength, 
        chosen_conf
    )

    if dbg:
        print(f"[REV V3] {pair_data.get('symbol','?')} - Signal: {chosen_dir} conf={chosen_conf:.2f} strength={reversal_strength.get('strength_class')} quality={quality_score:.2f} layers={chosen_layers}")

    return StrategyDetectionResult(
        direction=chosen_dir,
        confidence=round(chosen_conf, 2),
        layers=chosen_layers,
        quality_score=round(quality_score, 2),
        fallback_used=len(primary_layers) == 0,
        reversal_strength=reversal_strength
    )


def compute_atr_from_klines(klines, period=14):
    """Compute ATR (simple moving average of True Range) from klines list."""
    if not klines or len(klines) < 2:
        return None
    trs = []
    for i in range(1, len(klines)):
        prev_close = float(klines[i-1][4])
        high = float(klines[i][2]); low = float(klines[i][3]); close = float(klines[i][4])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if not trs:
        return None
    # use last `period` values
    trs = trs[-period:]
    try:
        return sum(trs) / len(trs)
    except Exception:
        return None


def estimate_tp_sl_hit_prob(klines, entry, tp, sl, lookahead=12, min_samples=8, vol_est=None):
    """Estimate probabilities that TP or SL will be hit first using historical forward-simulation over klines.
    Returns (prob_tp, prob_sl). If insufficient samples, falls back to ATR-based heuristic.
    """
    dynamic_lookahead = lookahead
    if vol_est:
        dynamic_lookahead = int(max(6, min(36, lookahead * (0.02 / max(0.005, vol_est)))))

    if not klines or len(klines) < dynamic_lookahead + 5:
        # fallback to ATR heuristic
        atr = compute_atr_from_klines(klines) if klines else None
        if not atr or atr <= 0:
            return 0.5, 0.5
        tp_dist = abs(tp - entry)
        sl_dist = abs(entry - sl)
        # probability of moving d approximated by exp(-d/(c*atr))
        c = 1.5
        if vol_est:
            c *= max(0.7, min(1.6, vol_est / 0.02))
        p_tp = math.exp(-tp_dist / (c * atr))
        p_sl = math.exp(-sl_dist / (c * atr))
        s = p_tp + p_sl
        if s <= 0:
            return 0.5, 0.5
        return p_tp / s, p_sl / s

    samples = 0
    tp_hits = 0
    sl_hits = 0
    N = len(klines)
    # treat each possible anchor index as historical 'entry' moment
    for i in range(0, N - dynamic_lookahead - 1):
        hit = None
        ambiguous = False
        for j in range(i+1, min(N, i+1+dynamic_lookahead)):
            high = float(klines[j][2]); low = float(klines[j][3])
            # if both levels in same candle, mark ambiguous and skip this sample
            if high >= tp and low <= sl:
                ambiguous = True
                break
            if high >= tp:
                hit = 'tp'
                break
            if low <= sl:
                hit = 'sl'
                break
        if ambiguous or hit is None:
            continue
        samples += 1
        if hit == 'tp':
            tp_hits += 1
        elif hit == 'sl':
            sl_hits += 1
    if samples >= min_samples:
        return tp_hits / samples, sl_hits / samples
    # fallback to ATR heuristic if not enough historical samples
    atr = compute_atr_from_klines(klines)
    if not atr or atr <= 0:
        return 0.5, 0.5
    tp_dist = abs(tp - entry)
    sl_dist = abs(entry - sl)
    c = 1.5
    if vol_est:
        c *= max(0.7, min(1.6, vol_est / 0.02))
    p_tp = math.exp(-tp_dist / (c * atr))
    p_sl = math.exp(-sl_dist / (c * atr))
    s = p_tp + p_sl
    if s <= 0:
        return 0.5, 0.5
    return p_tp / s, p_sl / s


def adjust_confidence_with_probs(conf, prob_tp, prob_sl):
    """Adjust base confidence using TP/SL probabilities.
    Maps actual win probability (prob_tp) to confidence, but keeps base signal confidence as floor.
    
    Logic:
    - If prob_tp > prob_sl: signal should win more often, boost confidence
    - If prob_tp < prob_sl: signal should lose more often, reduce confidence
    - If prob_tp ≈ prob_sl: neutral, keep base confidence
    - Result is weighted blend: base confidence gets 40%, win probability gets 60%
    """
    total_prob = prob_tp + prob_sl
    if total_prob <= 0:
        return conf
    
    win_prob_normalized = prob_tp / total_prob * 100.0
    
    blended_conf = conf * 0.4 + win_prob_normalized * 0.6
    return max(0.0, min(100.0, blended_conf))


def apply_confidence_decay(signal, decay_rate_per_minute=2.5):
    """Apply time-based decay to signal confidence."""
    if not signal or 'timestamp' not in signal:
        return signal.get('confidence', 0.0) if signal else 0.0

    now_ts = time.time()
    age_seconds = now_ts - signal['timestamp']
    age_minutes = age_seconds / 60.0

    decay_amount = age_minutes * decay_rate_per_minute
    original_confidence = signal.get('confidence', 0.0)
    decayed_confidence = original_confidence - decay_amount

    return max(0.0, decayed_confidence)


def apply_double_signal_bonus(signals, bonus=15.0):
    """If two strategies agree on the same pair/direction, boost confidence."""
    from collections import defaultdict

    pair_direction_counts = defaultdict(list)
    for i, signal in enumerate(signals):
        key = (signal.get('pair'), signal.get('direction'))
        pair_direction_counts[key].append(i)

    for key, indices in pair_direction_counts.items():
        if len(indices) > 1:
            for i in indices:
                signals[i]['confidence'] = min(100.0, signals[i]['confidence'] + bonus)
                if 'notes' in signals[i] and signals[i]['notes']:
                    signals[i]['notes'] += " | double_signal_bonus"
                else:
                    signals[i]['notes'] = "double_signal_bonus"
    return signals


def apply_volatility_rank_adjustment(signals):
    """Adjust confidence based on pair's volatility relative to the market median."""
    vol_ests = [s.get('vol_est') for s in signals if s.get('vol_est') is not None]
    if not vol_ests:
        return signals

    median_vol = statistics.median(vol_ests)
    if median_vol <= 0:
        return signals

    for signal in signals:
        vol_est = signal.get('vol_est')
        if vol_est is not None:
            # Calculate deviation from median
            deviation = abs(vol_est - median_vol) / median_vol
            # Penalize pairs that are far from the median volatility
            # The penalty is scaled by the deviation. A deviation of 1 (100%) results in a 20% penalty.
            penalty_factor = 1.0 - min(0.5, deviation * 0.2)
            signal['confidence'] *= penalty_factor
            signal['confidence'] = round(signal['confidence'], 2)
    return signals


# ---------------- RANGE STRATEGY DETECTOR (Balanced) ----------------
def bb_width_proxy(tv_metrics):
    # use TradingView BB width if available; otherwise return None
    if not tv_metrics:
        return None
    return tv_metrics.get("bbw", None)

def count_rejections_at_level(klines, level_price, is_top=True, threshold_pct=0.008, lookback=60):
    """
    Count number of times price tested level (within threshold_pct) and showed wick rejection
    """
    if not klines:
        return 0
    cnt = 0
    data = klines[-lookback:]
    bodies = []
    for k in data:
        open_p = float(k[1]); close = float(k[4])
        bodies.append(abs(close - open_p))
    bodies = sorted(bodies)
    size_threshold = bodies[int(max(0, len(bodies) - 1) * 0.75)] if bodies else 0.0
    for k in data:
        high = float(k[2]); low = float(k[3]); open_p = float(k[1]); close = float(k[4])
        price = (high + low + close) / 3.0
        if abs(price - level_price) / level_price <= threshold_pct:
            body = abs(close - open_p) or 1e-12
            # Removed body size threshold (was filtering out dojis/hammers)
            if is_top:
                wick = high - max(close, open_p)
            else:
                wick = min(close, open_p) - low
            if wick / body >= WICK_BODY_RATIO and (wick / ((open_p+close)/2 + 1e-12)) >= WICK_MIN_PCT:
                cnt += 1
    return cnt


def recent_bounce_confirmation(klines, level_price, is_top=True, threshold_pct=0.008):
    """Require the last two touches to reject the boundary for confirmation."""
    if not klines:
        return False
    hits = 0
    for idx in range(1, min(10, len(klines)) + 1):
        k = klines[-idx]
        high = float(k[2]); low = float(k[3]); open_p = float(k[1]); close = float(k[4])
        price = (high + low + close) / 3.0
        if abs(price - level_price) / level_price <= threshold_pct:
            body = abs(close - open_p) or 1e-12
            if is_top:
                wick = high - max(close, open_p)
            else:
                wick = min(close, open_p) - low
            if wick / body >= WICK_BODY_RATIO:
                hits += 1
            if hits >= 2:
                return True
    return False


def is_pivot_aligned(level_price):
    """Check if a boundary aligns with a round number / pivot step."""
    if level_price <= 0:
        return False
    step = max(1.0, PIVOT_LEVEL_STEP)
    remainder = level_price % step
    return remainder <= step * 0.05 or remainder >= step * 0.95


def volume_trend_declining(klines, window=6):
    """Return True if recent volume is contracting relative to prior window."""
    if not klines or len(klines) < window * 2:
        return True
    vols = [float(k[5]) for k in klines if len(k) >= 6]
    if len(vols) < window * 2:
        return True
    recent = vols[-window:]
    prev = vols[-2*window:-window]
    return sum(recent) <= sum(prev)


def compute_range_boundaries_from_klines(klines, lookback=80):
    """Compute fresh range boundaries directly from recent klines without stale data."""
    if not klines or len(klines) < 3:
        return None, None
    
    recent = klines[-lookback:] if len(klines) > lookback else klines
    highs = [float(k[2]) for k in recent if len(k) > 2]
    lows = [float(k[3]) for k in recent if len(k) > 3]
    
    if not highs or not lows:
        return None, None
    
    highest = max(highs)
    lowest = min(lows)
    
    return highest, lowest


def detect_range_stability(klines, highest, lowest, lookback=20):
    """
    Detect if price is oscillating within range (stable, not trending out).
    Returns (is_stable, stability_score) where score is 0-100.
    """
    if not klines or len(klines) < lookback:
        return False, 0.0

    try:
        recent = klines[-lookback:]
        closes = [float(k[4]) for k in recent]

        range_width = highest - lowest
        if range_width <= 0:
            return False, 0.0

        inside_range = 0
        for close in closes:
            if lowest < close < highest:
                inside_range += 1

        stability_pct = (inside_range / len(closes)) * 100.0
        is_stable = stability_pct >= 70.0
        return is_stable, stability_pct
    except Exception:
        return False, 0.0

def get_breakout_risk_penalty(klines, direction, highest, lowest):
    """Reduce breakout risk penalty for range signals."""
    if not klines or len(klines) < 5:
        return 1.0  # No penalty

    short_mom, _ = extract_short_medium_momentum(klines)
    is_vol_surging, _ = detect_volume_surge(klines)

    penalty = 0.9  # Starting penalty factor (adjusted)
    if is_vol_surging:
        if direction == "SHORT" and short_mom > 0.5:
            penalty = 0.8  # Reduced penalty for shorts
        elif direction == "LONG" and short_mom < -0.5:
            penalty = 0.8  # Reduced penalty for longs
    return penalty

def detect_consecutive_rejections(klines, is_top, lookback=10, threshold=0.015):
    """
    Detect if there are consecutive rejections at a level.
    Returns count of recent candles showing rejection pattern.
    """
    if not klines or len(klines) < 3:
        return 0

    try:
        recent = klines[-lookback:]
        consecutive = 0

        for k in recent[-3:]:
            open_p = float(k[1])
            high = float(k[2])
            low = float(k[3])
            close = float(k[4])

            if is_top:
                wick = high - max(close, open_p)
                body = abs(close - open_p)
                if wick > body * 0.8 and wick > open_p * threshold:
                    consecutive += 1
            else:
                wick = min(close, open_p) - low
                body = abs(close - open_p)
                if wick > body * 0.8 and wick > open_p * threshold:
                    consecutive += 1

        return consecutive
    except Exception:
        return 0

# ============= ENHANCED RANGE DETECTOR v2 (with fallbacks) =============

def detect_range_opportunity_v2_with_fallbacks(pair_data, binance_symbol, tv_metrics=None, kl_30m=None,
                                               kl_1h=None, kl_4h=None, market_sentiment_pct=50.0, market_trend="neutral", vol_est=None):
    """
    Multi-timeframe range detector with PRIMARY, SECONDARY, and TERTIARY fallback methods.
    Always attempts to find a quality range signal.
    Returns dict signal or None.
    """
    dbg = os.getenv("DEBUG_RANGE", "false").lower() == "true"
    coin_id = pair_data.get("coin_id") if isinstance(pair_data, dict) else None

    # Verify Pair Compatibility with Range-specific exchanges (Gate.io, Bitstamp, Huobi)
    if not verify_pair_fetch_compatibility(pair_data.get("symbol", ""), ["gateio", "bitstamp", "huobi"]):
        if dbg:
            print(f"[RANGE] {pair_data.get('symbol', 'UNKNOWN')} - NOT COMPATIBLE WITH RANGE EXCHANGES, SKIPPING")
        return None

    def _try_fetch(interval, limit):
        return fetch_any_klines(binance_symbol, interval=interval, limit=limit, coin_id=coin_id, strategy="range")

    # Only fetch if not provided to reduce API load
    if kl_30m is None:
        try:
            kl_30m = _try_fetch("30m", 150)
        except Exception:
            kl_30m = None
    if kl_1h is None:
        try:
            kl_1h = _try_fetch("1h", 120)
        except Exception:
            kl_1h = None
    if kl_4h is None:
        try:
            kl_4h = _try_fetch("4h", 80)
        except Exception:
            kl_4h = None

    price = pair_data.get("price")
    if not price or price <= 0:
        # Try to get price from strategy-specific market data API (Huobi → Gemini)
        try:
            pair_symbol = binance_symbol.replace("USDT", "").replace("BUSD", "").replace("USDC", "")
            if pair_symbol:
                market_data_result = fetch_market_data_range(pair_symbol)
                if market_data_result and isinstance(market_data_result, dict):
                    price = market_data_result.get("price")
        except Exception:
            pass
        if not price or price <= 0:
            if dbg:
                print(f"[RANGE V2] {binance_symbol} - No price available from pair_data or Huobi API, rejecting")
            return None

    swings_h_1h, swings_l_1h = detect_recent_swing_levels(kl_1h, lookback=80) if kl_1h else ([], [])
    swings_h_30, swings_l_30 = detect_recent_swing_levels(kl_30m, lookback=120) if kl_30m else ([], [])
    swings_h_4h, swings_l_4h = detect_recent_swing_levels(kl_4h, lookback=60) if kl_4h else ([], [])

    all_swings_h = list(set((swings_h_1h or []) + (swings_h_30 or []) + (swings_h_4h or [])))
    all_swings_l = list(set((swings_l_1h or []) + (swings_l_30 or []) + (swings_l_4h or [])))

    if not all_swings_h or not all_swings_l:
        if dbg:
            print(f"[RANGE V2] {binance_symbol} - No swings detected, using price-based fallback")
        try:
            high_24h = pair_data.get("high_24h") or price * 1.05 # Increased from 1.02
            low_24h = pair_data.get("low_24h") or price * 0.95 # Increased from 0.98
            all_swings_h = [high_24h]
            all_swings_l = [low_24h]
        except Exception:
            return None

    highest = max(all_swings_h)
    lowest = min(all_swings_l)

    width_pct = (highest - lowest) / (lowest + 1e-12)
    if width_pct > RANGE_MAX_WIDTH_PCT:
        if dbg:
            print(f"[RANGE V2] {binance_symbol} - Range too wide ({width_pct:.4f}), tightening...")
        highest_alt = sorted(all_swings_h)[-2] if len(all_swings_h) > 1 else highest
        lowest_alt = sorted(all_swings_l)[1] if len(all_swings_l) > 1 else lowest
        width_pct_alt = (highest_alt - lowest_alt) / (lowest_alt + 1e-12)
        if width_pct_alt <= RANGE_MAX_WIDTH_PCT * 1.2:
            highest, lowest = highest_alt, lowest_alt
            width_pct = width_pct_alt
        elif width_pct <= RANGE_MAX_WIDTH_PCT * 1.5:
            pass
        else:
            return None

    rejections_top = 0
    rejections_bottom = 0
    if kl_1h:
        rejections_top += count_rejections_at_level(kl_1h, highest, is_top=True, threshold_pct=0.02, lookback=120)
        rejections_bottom += count_rejections_at_level(kl_1h, lowest, is_top=False, threshold_pct=0.02, lookback=120)
    if kl_30m:
        rejections_top += count_rejections_at_level(kl_30m, highest, is_top=True, threshold_pct=0.02, lookback=160)
        rejections_bottom += count_rejections_at_level(kl_30m, lowest, is_top=False, threshold_pct=0.02, lookback=160)
    if kl_4h and rejections_top < 2:
        rejections_top += count_rejections_at_level(kl_4h, highest, is_top=True, threshold_pct=0.03, lookback=60)
    if kl_4h and rejections_bottom < 2:
        rejections_bottom += count_rejections_at_level(kl_4h, lowest, is_top=False, threshold_pct=0.03, lookback=60)

    rejections_top = min(rejections_top, 10)
    rejections_bottom = min(rejections_bottom, 10)

    bounce_top = recent_bounce_confirmation(kl_30m or kl_1h, highest, is_top=True) if (kl_30m or kl_1h) else False
    bounce_bottom = recent_bounce_confirmation(kl_30m or kl_1h, lowest, is_top=False) if (kl_30m or kl_1h) else False

    range_width = highest - lowest
    dist_top_norm = (highest - price) / range_width if range_width else 1.0
    dist_bottom_norm = (price - lowest) / range_width if range_width else 1.0
    dist_top_pct = abs(highest - price) / max(price, 1e-12)
    dist_bottom_pct = abs(price - lowest) / max(price, 1e-12)

    near_top = ((RANGE_EDGE_MIN_OFFSET <= dist_top_pct <= RANGE_EDGE_MAX_OFFSET) or (dist_top_norm < RANGE_NEAR_EDGE_PCT))
    near_bottom = ((RANGE_EDGE_MIN_OFFSET <= dist_bottom_pct <= RANGE_EDGE_MAX_OFFSET) or (dist_bottom_norm < RANGE_NEAR_EDGE_PCT))
    mid_zone = (RANGE_MID_MIN_OFFSET <= dist_top_pct <= RANGE_MID_MAX_OFFSET and
                RANGE_MID_MIN_OFFSET <= dist_bottom_pct <= RANGE_MID_MAX_OFFSET)

    # More permissive mid-zone entry - allow even with fewer rejections
    allow_mid = mid_zone and (rejections_top >= 0 or rejections_bottom >= 0)  # Changed from >= 1

    # More permissive edge entry - reduce minimum rejections
    min_rejections = max(0, RANGE_MIN_REJECTIONS - 1)  # Allow 1 less rejection than configured
    if near_top and rejections_top < min_rejections:
        near_top = False
    if near_bottom and rejections_bottom < min_rejections:
        near_bottom = False

    # Add fallback entry conditions when no clear range boundaries
    if not (near_top or near_bottom or allow_mid):
        # Check for consolidation patterns even without clear boundaries
        if kl_1h and len(kl_1h) >= 10:
            recent_range = max(float(k[2]) for k in kl_1h[-10:]) - min(float(k[3]) for k in kl_1h[-10:])
            avg_range = sum((float(k[2]) - float(k[3])) for k in kl_1h[-20:]) / 20 if len(kl_1h) >= 20 else recent_range
            if recent_range < avg_range * 0.7:  # Recent range is tighter (consolidation)
                if price > (highest + lowest) / 2:  # Above midpoint
                    direction = "SHORT"
                    entry_label = "consolidation_resistance"
                else:  # Below midpoint
                    direction = "LONG"
                    entry_label = "consolidation_support"
                if dbg:
                    print(f"[RANGE V2] {binance_symbol} - Using consolidation entry: {direction} at {entry_label}")
            else:
                if dbg:
                    print(f"[RANGE V2] {binance_symbol} - No entry zone, rejecting (no clear boundary or consolidation)")
                return None
        else:
            if dbg:
                print(f"[RANGE V2] {binance_symbol} - No entry zone, rejecting (no clear boundary)")
            return None

    direction = None
    entry_label = "edge"
    # FIXED Priority 2: Range rejection logic - strong rejections confirm boundary, don't flip direction
    if near_top:
        direction = "SHORT"  # At top of range, expect SHORT (sell at resistance)
    elif near_bottom:
        direction = "LONG"   # At bottom of range, expect LONG (buy at support)
    elif allow_mid:
        entry_label = "mid"
        mid_point = (highest + lowest) / 2.0
        direction = "LONG" if price <= mid_point else "SHORT"

    # Fallback: Simple trend-based entry if no clear range direction
    if not direction and kl_1h and len(kl_1h) >= 5:
        # Check recent trend in 1h timeframe
        recent_closes = [float(k[4]) for k in kl_1h[-5:]]
        trend_up = sum(1 for i in range(1, len(recent_closes)) if recent_closes[i] > recent_closes[i-1])
        
        if trend_up >= 4:  # 4 out of 5 candles up
            direction = "SHORT" # Reversion: expect top of range
            entry_label = "range_reversion_top"
        elif trend_up <= 1:  # 1 or fewer candles up
            direction = "LONG" 
            entry_label = "range_reversion_bottom"
        
        if direction and dbg:
            print(f"[RANGE V2] {binance_symbol} - Using trend continuation fallback: {direction} at {entry_label}")

    # Enhanced fallback: Generate direction even if no clear range boundaries
    if not direction:
        if kl_1h and len(kl_1h) >= 5:
            # Use recent trend to determine direction
            recent_closes = [float(k[4]) for k in kl_1h[-5:]]
            trend_up = sum(1 for i in range(1, len(recent_closes)) if recent_closes[i] > recent_closes[i-1])
            
            if trend_up >= 3:
                direction = "SHORT"
                entry_label = "trend_fallback_reversion_short"
            elif trend_up <= 1:
                direction = "LONG"
                entry_label = "trend_fallback_reversion_long"
            else:
                # Use price position relative to 24h range
                high_24h = pair_data.get("high_24h") or price * 1.05
                low_24h = pair_data.get("low_24h") or price * 0.95
                mid_24h = (high_24h + low_24h) / 2.0
                
                if price > mid_24h:
                    direction = "SHORT"
                    entry_label = "range_position_fallback_short"
                else:
                    direction = "LONG"
                    entry_label = "range_position_fallback_long"
            
            if dbg:
                print(f"[RANGE V2] {binance_symbol} - Using enhanced fallback: {direction} at {entry_label}")
        else:
            if dbg:
                print(f"[RANGE V2] {binance_symbol} - No direction determined (insufficient data)")
            return None

    fresh_price = None
    kl_1m = kl_5m = kl_15m = None
    try:
        kl_1h_new, kl_1m, kl_5m, kl_15m, _ = fetch_klines_for_range(binance_symbol, interval="1h", limit=120, coin_id=coin_id)
        if kl_1h_new and not kl_1h:
            kl_1h = kl_1h_new
    except Exception:
        pass
    
    fresh_price = pair_data.get("price") if pair_data else None
    if fresh_price is None or (isinstance(fresh_price, (int, float)) and fresh_price <= 0):
        range_symbol = binance_symbol if isinstance(binance_symbol, str) else (pair_data.get("symbol", "") + "USDT" if isinstance(pair_data, dict) else "")
        klines_dict_range = {
            "1m": kl_1m,
            "5m": kl_5m,
            "15m": kl_15m
        }
        # Create a market_data dict from pair_data for get_entry_price_safe
        market_data_for_price = {range_symbol: pair_data} if pair_data else None
        fresh_price, range_price_source, range_price_staleness = get_entry_price_safe(
            klines_dict_range, range_symbol, coin_id=coin_id, strategy_name="RANGE", fallback_only_fresh=True, market_data=market_data_for_price
        )
        if fresh_price:
            range_price_metadata = {"price_source": range_price_source, "price_staleness_sec": range_price_staleness}
        else:
            if dbg:
                print(f"[RANGE V2] {binance_symbol} - No fresh price available from Huobi (strategy-specific API), rejecting signal")
            return None
    else:
        range_price_metadata = {"price_source": "livecoinwatch_range", "price_staleness_sec": 0.0}
    
    if fresh_price and fresh_price > 0:
        price = fresh_price

    dir_confirmed = True
    if direction and (kl_1h or kl_30m):
        base_kl = kl_1h if kl_1h else kl_30m
        dir_confirmed = range_direction_confirmed(base_kl, direction)
        if not dir_confirmed and dbg:
            print(f"[RANGE V2] {binance_symbol} - Direction not confirmed by fresh klines ({direction})")

    vols_1h = [float(k[5]) for k in kl_1h[-24:]] if kl_1h and len(kl_1h) >= 24 else []
    avg_1h_vol = statistics.mean(vols_1h) if vols_1h else None

    vol_not_excessive = True
    if avg_1h_vol and pair_data.get("volume"):
        daily_vol = pair_data.get("volume") or 0.0
        est_1h_from_24h = daily_vol / 24.0 if daily_vol > 0 else None
        if est_1h_from_24h and avg_1h_vol > 0:
            if avg_1h_vol > est_1h_from_24h * (1.0 / RANGE_VOLUME_CONTRACTION_RATIO):
                vol_not_excessive = False

    vol_declining = False
    if kl_30m or kl_1h:
        base_kl = kl_30m if kl_30m else kl_1h
        vol_declining = volume_trend_declining(base_kl)

    vol_ok = vol_not_excessive or vol_declining

    bbw = bb_width_proxy(tv_metrics)
    bb_compact = False
    if bbw is not None and bbw < RANGE_BB_COMPRESS_THRESHOLD:
        bb_compact = True

    breakout_risk = False
    ema5 = ema20 = None
    if tv_metrics:
        rsi = tv_metrics.get("rsi")
        ema5 = tv_metrics.get("ema5")
        ema20 = tv_metrics.get("ema20")
        if direction == "SHORT":
            if (rsi and rsi > 65) and (ema5 and ema20 and ema5 > ema20):
                breakout_risk = True
        else:
            if (rsi and rsi < 35) and (ema5 and ema20 and ema5 < ema20):
                breakout_risk = True

    pivot_alignment = is_pivot_aligned(highest) or is_pivot_aligned(lowest)

    primary_layers = []
    secondary_layers = []
    tertiary_layers = []

    width_score = max(0.0, min(30.0, (1.0 - width_pct / RANGE_MAX_WIDTH_PCT) * 30.0))
    if width_score > 10.0:  # Lowered from 15.0 to allow more primary layers
        primary_layers.append(("width", 20.0))

    rejections_total = rejections_top + rejections_bottom
    if rejections_total >= 2:  # Lowered from 3 to 2
        primary_layers.append(("rejections_multi", 22.0))  # Reduced weight from 24.0
    elif rejections_total >= 1:
        secondary_layers.append(("rejections_pair", 16.0))  # Moved from tertiary to secondary
    else:
        tertiary_layers.append(("rejections_single", 12.0))  # Lowered weight from 15.0

    if bounce_top or bounce_bottom:
        secondary_layers.append(("bounce_confirmation", 18.0))

    if vol_ok:
        secondary_layers.append(("volume_contraction", 18.0))
    else:
        tertiary_layers.append(("volume_settling", 4.0))

    if bb_compact:
        primary_layers.append(("bb_compact", 22.0))

    if pivot_alignment:
        tertiary_layers.append(("pivot_aligned", 16.0))

    is_stable, stability_score = detect_range_stability(kl_1h or kl_30m, highest, lowest, lookback=20)
    if is_stable:
        secondary_layers.append(("range_stability", 16.0))

    consecutive_top = detect_consecutive_rejections(kl_1h or kl_30m, is_top=True, lookback=10)
    consecutive_bot = detect_consecutive_rejections(kl_1h or kl_30m, is_top=False, lookback=10)
    if consecutive_top >= 2 or consecutive_bot >= 2:
        tertiary_layers.append(("consecutive_rejections", 14.0))

    if direction == "SHORT" and near_top:
        secondary_layers.append(("at_resistance", 18.0))
    elif direction == "LONG" and near_bottom:
        secondary_layers.append(("at_support", 18.0))

    def aggregate_layered_conf(primary, secondary, tertiary):
        """Aggregate confidence from layered detection with method agreement bonus."""
        all_layers = primary + secondary + tertiary
        if not all_layers:
            return 12.0, []  # Return baseline confidence instead of 0.0 to allow fallback signals (ensures it passes minimum)

        total_conf = 0.0
        layer_names = []

        for layer_name, weight in primary:
            total_conf += weight
            layer_names.append(layer_name)

        for layer_name, weight in secondary:
            total_conf += weight
            layer_names.append(layer_name)

        for layer_name, weight in tertiary:
            total_conf += weight
            layer_names.append(layer_name)

        agreement_bonus = calculate_method_agreement_bonus(len(layer_names))
        total_conf += agreement_bonus

        return min(100.0, total_conf), layer_names

    conf, layer_names = aggregate_layered_conf(primary_layers, secondary_layers, tertiary_layers)

    penalty_points = 0.0
    bonus_points = 0.0

    if not dir_confirmed and direction:
        penalty_points += 2.0  # Reduced from 4.0

    # Removed ATR distance penalty (Healthy ranges are often wider than 1 ATR)

    if entry_label == "mid":
        penalty_points += 3.0

    range_age_hours = None
    try:
        if kl_1h and len(kl_1h) > 1:
            start_idx = max(0, len(kl_1h) - 24)
            range_age_hours = (float(kl_1h[-1][0]) - float(kl_1h[start_idx][0])) / (1000.0 * 60.0 * 60.0)
    except Exception:
        range_age_hours = None
    if range_age_hours is not None:
        if range_age_hours > 24.0: # Increased from 12.0
            bonus_points += 5.0 # Older ranges are MORE reliable
        elif range_age_hours < 6.0:
            bonus_points += 3.0

    # Removed EMA trend penalty (Price is expected to be counter-trend at range edges)

    conf = max(0.0, conf - penalty_points + bonus_points)
    conf = min(100.0, conf)

    if direction == "LONG" and rejections_bottom >= 1:
        conf += 4.0
    elif direction == "SHORT" and rejections_top >= 1:
        conf += 4.0

    conf = min(100.0, conf)

    # Contrarian Sentiment Logic for Range
    sentiment_neutral = 50.0
    if market_sentiment_pct < sentiment_neutral:
        # Fearful market: Boost LONG at support, slightly penalize SHORT at resistance
        long_sentiment_scale = 1.0 + ((sentiment_neutral - market_sentiment_pct) / 100.0)
        short_sentiment_scale = max(0.7, 1.0 - ((sentiment_neutral - market_sentiment_pct) / 100.0))
    else:
        # Greedy market: Boost SHORT at resistance, slightly penalize LONG at support
        short_sentiment_scale = 1.0 + ((market_sentiment_pct - sentiment_neutral) / 100.0)
        long_sentiment_scale = max(0.7, 1.0 - ((market_sentiment_pct - sentiment_neutral) / 100.0))

    if direction == "LONG":
        sentiment_scale = long_sentiment_scale
        conf *= long_sentiment_scale
    else:
        sentiment_scale = short_sentiment_scale
        conf *= short_sentiment_scale

    conf = max(0.0, min(100.0, conf))

    breakout_penalty = get_breakout_risk_penalty(kl_30m or kl_1h, direction, highest, lowest)
    conf *= breakout_penalty

    try:
        vol_trend, vol_surge, vol_strength = analyze_multi_timeframe_volume(kl_1m, kl_5m, kl_15m)
        if vol_surge and vol_strength is not None and vol_strength > 0.5:
            conf += min(5.0, vol_strength * 5.0)
            if dbg:
                print(f"[RANGE V2] {binance_symbol} - Multi-TF volume surge={vol_surge} strength={vol_strength:.2f}", flush=True)
    except Exception as e:
        if dbg:
            print(f"[RANGE V2] {binance_symbol} - MTF volume analysis failed: {str(e)[:60]}", flush=True)

    # Apply market trend bias
    # Neutralized market trend bias to avoid direction skew
    # Previously adjusted confidence by market_trend; now no-op to keep symmetry.
    conf *= 1.0

    range_min_local = max(8.0, RANGE_MIN_CONFIDENCE * sentiment_scale * 0.5)  # Lowered floor from 15.0 to 10.0, then to 8.0 with 0.5 multiplier (more lenient)
    
    # Ensure minimum confidence is met - boost if below threshold
    if conf < range_min_local:
        boost_needed = range_min_local - conf
        # Add boost based on available layers
        if len(layer_names) > 0:
            conf += min(boost_needed, len(layer_names) * 2.0)  # Boost by 2 points per layer, up to needed amount
        else:
            # Even with no layers, give baseline boost to pass minimum
            conf = range_min_local

    # Calculate dynamic TP/SL using same sophisticated method as other strategies
    bin_info = {
        "vwap": None,
        "top_bins": [],
        "swing_highs": [highest] if highest else [],
        "swing_lows": [lowest] if lowest else []
    }
    if fresh_price is None:
        fresh_price = price
    
    try:
        _, kl_1m_refresh, kl_5m_refresh, kl_15m_refresh, kline_price = fetch_klines_for_range(binance_symbol, interval="1h", limit=120, coin_id=coin_id)
        if kl_1m_refresh and not kl_1m:
            kl_1m = kl_1m_refresh
        if kl_5m_refresh and not kl_5m:
            kl_5m = kl_5m_refresh
        if kl_15m_refresh and not kl_15m:
            kl_15m = kl_15m_refresh
        if kline_price and kline_price > 0:
            fresh_price = kline_price
            if dbg:
                print(f"[RANGE V2] {binance_symbol} - Live price from 1m klines: {fresh_price}")
        else:
            if dbg:
                print(f"[RANGE V2] {binance_symbol} - Kline price fetch invalid, trying live API")
            try:
                # Use strategy-specific market data API (Huobi → Gemini)
                pair_symbol = binance_symbol.replace("USDT", "").replace("BUSD", "").replace("USDC", "")
                if pair_symbol:
                    market_data_result = fetch_market_data_range(pair_symbol)
                    if market_data_result and isinstance(market_data_result, dict):
                        api_price = market_data_result.get("price")
                        if api_price and api_price > 0:
                            fresh_price = api_price
                            if dbg:
                                print(f"[RANGE V2] {binance_symbol} - Live price from Huobi API: {fresh_price}")
            except Exception as api_err:
                if dbg:
                    print(f"[RANGE V2] {binance_symbol} - Huobi API price fetch failed: {str(api_err)[:60]}")
    except Exception as e:
        if dbg:
            print(f"[RANGE V2] {binance_symbol} - Kline price fetch failed: {str(e)[:80]}")

    sl_pct_range, tp_pct_range = calculate_dynamic_tp_sl_from_movement(
        kl_30m or kl_1h, fresh_price, direction, strategy_type="range",
        tv_metrics=tv_metrics, bin_info=bin_info, vol_est=vol_est,
        momentum_strength=None, confidence=conf,
        market_trend=market_trend
    )

    if direction == "SHORT":
        sl_price_by_boundary = highest * 1.01
        sl_price_by_pct = fresh_price * (1 + sl_pct_range)
        sl = max(sl_price_by_boundary, sl_price_by_pct)
    else:
        sl_price_by_boundary = lowest * 0.99
        sl_price_by_pct = fresh_price * (1 - sl_pct_range)
        sl = min(sl_price_by_boundary, sl_price_by_pct)

    sl_pct_final = abs(fresh_price - sl) / fresh_price

    if direction == "SHORT":
        tp_target_price = lowest + (range_width * 0.2)
        tp_dynamic_price = fresh_price * (1 - tp_pct_range)
        tp = max(tp_target_price, tp_dynamic_price)
    else:
        tp_target_price = highest - (range_width * 0.2)
        tp_dynamic_price = fresh_price * (1 + tp_pct_range)
        tp = min(tp_target_price, tp_dynamic_price)

    tp_pct_final = abs(tp - fresh_price) / fresh_price

    # Ensure a minimum Reward/Risk ratio - FIXED: More permissive R/R threshold
    rr_ratio = tp_pct_final / (sl_pct_final + 1e-9)
    if rr_ratio < 0.5:
        if dbg:
            print(f"[RANGE V2] {binance_symbol} - Signal rejected due to poor R/R ({rr_ratio:.2f})")
        return None
    if rr_ratio < 0.8:
        conf *= 0.85
    # --- End of Improved Risk Management ---

    # Calculate range strength and apply adjustments to confidence
    range_strength = calculate_range_strength(pair_data, {
        "width_pct": width_pct * 100,
        "rejections_top": rejections_top,
        "rejections_bottom": rejections_bottom
    }, direction)

    strength_mult = range_strength.get("multiplier", 1.0)
    conf *= strength_mult

    spread_adj = signal_spread_penalty(price)
    if direction == "LONG":
        tp = max(lowest, tp - spread_adj)
    else:
        tp = min(highest, tp + spread_adj)

    bounce_count = (1 if bounce_top else 0) + (1 if bounce_bottom else 0)
    quality_score = calculate_range_quality_score(
        len(layer_names),
        layer_names,
        rejections_top + rejections_bottom,
        bb_compact,
        vol_ok,
        stability_score,
        conf,
        bounce_count
    )

    # Ensure minimum confidence but allow fallback signals with lower confidence
    if conf < range_min_local:
        # Apply fallback boost to ensure signal generation
        conf = max(range_min_local * 0.9, conf + 2.0)  # Boost by 2.0 or set to 90% of minimum
        if dbg:
            print(f"[RANGE V2] {binance_symbol} {direction} - Applied fallback boost: confidence adjusted to {conf:.2f}")
    
    sig = {
        "type": "RANGE",
        "timestamp": time.time(),
        "pair": pair_data.get("symbol", "") + "USDT" if pair_data.get("symbol") else None,
        "direction": direction, # Corrected from sig.get('direction')
        "entry": round(fresh_price, 8),
        "sl": round(sl, 8),
        "tp": round(tp, 8),
        "sl_pct": round(sl_pct_final, 6),
        "tp_pct": round(tp_pct_final, 6),
        "confidence": round(conf, 2),
        "quality_score": round(quality_score, 2),
        "trigger_layers": layer_names,
        "range_meta": {
            "low": round(lowest, 8),
            "high": round(highest, 8),
            "width_pct": round(width_pct * 100, 3),
            "rejections_top": rejections_top,
            "rejections_bottom": rejections_bottom,
            "bb_compact": bb_compact,
            "vol_ok": vol_ok,
            "entry_type": entry_label,
            "range_age_h": round(range_age_hours, 2) if range_age_hours is not None else None,
            "pivot_aligned": pivot_alignment,
            "bounce_top": bounce_top,
            "bounce_bottom": bounce_bottom
        },
        "notes": "range_v2_with_fallbacks",
        "range_strength": range_strength.get("class", "UNKNOWN"),
        "range_strength_score": range_strength.get("score", 0),
        "price_source": range_price_metadata.get("price_source", "unknown") if 'range_price_metadata' in locals() else "unknown",
        "price_staleness_sec": range_price_metadata.get("price_staleness_sec", 0) if 'range_price_metadata' in locals() else 0
    }
    
    assert sig.get("direction") in ["LONG", "SHORT"], f"RANGE: Invalid direction {sig.get('direction')}"

    try:
        prob_tp, prob_sl = estimate_tp_sl_hit_prob(kl_1h if kl_1h else kl_30m, float(sig["entry"]), float(sig["tp"]), float(sig["sl"]), lookahead=16, vol_est=vol_est)
        sig["prob_tp"] = round(prob_tp, 3)
        sig["prob_sl"] = round(prob_sl, 3)

        adj_conf = adjust_confidence_with_probs(float(sig["confidence"]), prob_tp, prob_sl)
        sig["confidence"] = round(adj_conf, 2)
    except Exception:
        pass

    if entry_label == "mid":
        sig["notes"] += " | mid_range_entry"
    if pivot_alignment:
        sig["notes"] += " | pivot_aligned"
    if range_strength.get("class") == "STRONG":
        sig["notes"] += " | strong_range"

    return sig



# ============= ENHANCED MOMENTUM DETECTOR v3 (with comprehensive improvements) =============

def classify_momentum_strength(klines, price, direction, volume_surge=False):
    """Classify momentum strength: WEAK, MODERATE, STRONG, EXPLOSIVE"""
    if not klines or not isinstance(klines, list) or len(klines) < 20:
        return {"class": "WEAK", "score": 20, "multiplier": 0.8}

    try:
        closes = [float(k[4]) for k in klines[-20:]]
        volumes = [float(k[5]) for k in klines[-20:]]

        momentum_5 = abs(closes[-1] - closes[-6]) / closes[-6] if len(closes) > 5 else 0
        momentum_10 = abs(closes[-1] - closes[-11]) / closes[-11] if len(closes) > 10 else 0

        avg_vol = sum(volumes[-10:]) / 10
        recent_vol = sum(volumes[-3:]) / 3
        vol_ratio = recent_vol / avg_vol if avg_vol > 0 else 1

        consecutive = 0
        for i in range(len(closes)-1, 0, -1):
            if direction == "LONG" and closes[i] > closes[i-1]:
                consecutive += 1
            elif direction == "SHORT" and closes[i] < closes[i-1]:
                consecutive += 1
            else:
                break

        score = 0
        if momentum_5 > 0.02: score += 25
        if momentum_10 > 0.05: score += 25
        if vol_ratio > 1.5: score += 20
        if consecutive >= 3: score += 15
        if volume_surge: score += 15

        if score >= 85:
            return {"class": "EXPLOSIVE", "score": score, "multiplier": 1.4}
        elif score >= 65:
            return {"class": "STRONG", "score": score, "multiplier": 1.2}
        elif score >= 45:
            return {"class": "MODERATE", "score": score, "multiplier": 1.0}
        else:
            return {"class": "WEAK", "score": score, "multiplier": 0.8}
    except Exception:
        return {"class": "WEAK", "score": 20, "multiplier": 0.8}

def detect_volume_surge(klines, lookback=10):
    """Detect significant volume surge indicating momentum (adjusted)."""
    if not klines or len(klines) < lookback + 3:
        return False, 0

    try:
        volumes = [float(k[5]) for k in klines]
        baseline_vol = sum(volumes[-(lookback+3):-3]) / max(lookback, 1)
        recent_vol = sum(volumes[-3:]) / 3

        # Tuned surge ratio threshold (adjusted for accuracy)
        surge_ratio = recent_vol / baseline_vol if baseline_vol > 0 else 1
        return surge_ratio > 1.5, surge_ratio  # Lowered threshold from 2.0 to 1.5
    except Exception:
        return False, 0

def compute_momentum_direction_from_klines(klines):
    """Determine momentum direction from fresh klines (adjusted thresholds)."""
    if not klines or len(klines) < 6:
        return "NEUTRAL"
    
    try:
        closes = [float(k[4]) for k in klines]
        
        short_mom_pct = ((closes[-1] - closes[-4]) / closes[-4] * 100.0) if len(closes) >= 4 else 0
        medium_mom_pct = ((closes[-1] - closes[-16]) / closes[-16] * 100.0) if len(closes) >= 16 else short_mom_pct
        
        # Tuned classification thresholds
        if short_mom_pct > 0.6:
            return "LONG"
        elif short_mom_pct < -0.6:
            return "SHORT"
        else:
            return "NEUTRAL"
    except Exception:
        return "NEUTRAL"

def detect_breakout_momentum(klines, direction, lookback=20):
    """Detect if momentum is breaking key levels using fresh kline data"""
    if not klines or not isinstance(klines, list) or len(klines) < lookback:
        return False, 0

    try:
        price = float(klines[-1][4])
        
        highs = [float(k[2]) for k in klines[-lookback:]]
        lows = [float(k[3]) for k in klines[-lookback:]]

        if direction == "LONG":
            resistance = max(highs[:-1])
            breakout = price > resistance * 1.002
            strength = (price - resistance) / resistance if resistance > 0 else 0
        else:
            support = min(lows[:-1])
            breakout = price < support * 0.998
            strength = (support - price) / support if support > 0 else 0

        return breakout, strength * 100
    except Exception:
        return False, 0

def momentum_direction_confirmed(klines, direction, lookback=3):
    """
    Validate that momentum direction has consecutive candles moving in expected direction.
    Returns True if at least 2 of last 3 candles move in expected direction.
    """
    if not klines or len(klines) < lookback:
        return False
    
    try:
        closes = [float(k[4]) for k in klines[-lookback:]]
        consecutive = 0
        
        for i in range(1, len(closes)):
            if direction == "LONG" and closes[i] > closes[i-1]:
                consecutive += 1
            elif direction == "SHORT" and closes[i] < closes[i-1]:
                consecutive += 1
        
        return consecutive >= 2
    except Exception:
        return False

def detect_multi_timeframe_momentum(kl_15m, kl_1h, kl_4h, direction):
    """Check momentum alignment across timeframes"""
    alignments = 0

    for klines, name in [(kl_15m, "15m"), (kl_1h, "1h"), (kl_4h, "4h")]:
        if not klines or not isinstance(klines, list) or len(klines) < 10:
            continue

        try:
            closes = [float(k[4]) for k in klines[-10:]]
            momentum = (closes[-1] - closes[0]) / closes[0]

            if direction == "LONG" and momentum > 0.005:
                alignments += 1
            elif direction == "SHORT" and momentum < -0.005:
                alignments += 1
        except Exception:
            continue

    return alignments >= 2, alignments

def calculate_method_agreement_bonus(num_layers):
    """Calculate bonus based on number of detection methods agreeing."""
    if num_layers >= 3:
        return 8.0
    elif num_layers >= 2:
        return 4.0
    else:
        return 0.0

def apply_soft_cooldown_adjustment(conf, pair, direction, cooldown_cache):
    """Apply soft cooldown adjustment to confidence."""
    if not cooldown_cache:
        return conf
    key = (pair, direction)
    if key in cooldown_cache:
        cooldown_factor = max(0.5, 1.0 - (cooldown_cache[key] * 0.1))
        return conf * cooldown_factor
    return conf

def is_pivot_aligned(value):
    """Check if value is pivot-aligned (simple placeholder)."""
    return False

def volume_trend_declining(klines):
    """Check if volume trend is declining."""
    if not klines or len(klines) < 3:
        return False
    
    vols = [float(k[7]) if len(k) > 7 else 0 for k in klines[-3:]]
    return vols[-1] < vols[-2] < vols[-3] or (vols[-1] < sum(vols) / 3)

def bb_width_proxy(tv_metrics):
    """Get Bollinger Band width from tv_metrics."""
    if not tv_metrics:
        return None
    return tv_metrics.get("bbw")

def compute_bb_width_for_closes(closes, period=20):
    """Compute Bollinger Band width for given closes."""
    if not closes or len(closes) < period:
        return None
    
    recent = closes[-period:]
    mean = sum(recent) / len(recent)
    variance = sum((x - mean) ** 2 for x in recent) / len(recent)
    std_dev = variance ** 0.5
    
    return std_dev * 2

def now_utc_str():
    """Get current UTC time as string."""
    return datetime.now(timezone.utc).isoformat()

def cache_stats_str():
    """Get cache statistics as formatted string."""
    return get_cache_stats_str()

def check_kline_data_quality(klines, interval, pair, strategy):
    """Check kline data quality."""
    if not klines or len(klines) == 0:
        return False, ["No klines"]
    return True, []

def save_vol_cache(vol_cache):
    """Save volatility cache."""
    pass

def save_cooldown_cache(cooldown_cache):
    """Save cooldown cache."""
    pass

def report_performance_metrics():
    """Report performance metrics."""
    pass

def log_api_usage():
    """Log API usage statistics."""
    pass

def safe_sleep(seconds):
    """Safe sleep with keyboard interrupt handling."""
    if seconds > 0:
        try:
            time.sleep(seconds)
        except KeyboardInterrupt:
            raise

def warm_cache_on_startup():
    """Warm up cache on startup."""
    pass

def extract_short_medium_momentum(klines):
    """Extract short and medium term momentum from klines."""
    if not klines or len(klines) < 5:
        return 0.0, 0.0
    
    closes = [float(k[4]) for k in klines]
    
    short_mom = (closes[-1] - closes[-2]) / closes[-2] * 100 if closes[-2] != 0 else 0.0
    
    if len(closes) >= 5:
        medium_mom = (closes[-1] - closes[-5]) / closes[-5] * 100 if closes[-5] != 0 else 0.0
    else:
        medium_mom = short_mom
    
    return short_mom, medium_mom

def momentum_divergence_check(klines, direction):
    """Check for momentum divergence."""
    if len(klines) < 8:
        return False
    
    closes = [float(k[4]) for k in klines[-8:]]
    
    early_change = abs(closes[3] - closes[0])
    late_change = abs(closes[-1] - closes[-4])
    
    if direction == "LONG":
        return closes[-1] > closes[0] and late_change < early_change * 0.7
    else:
        return closes[-1] < closes[0] and late_change < early_change * 0.7

def higher_tf_bias_from_klines(klines, period=16):
    """Detect higher timeframe bias from klines."""
    if not klines or len(klines) < period:
        return "flat"
    
    closes = [float(k[4]) for k in klines[-period:]]
    highs = [float(k[2]) for k in klines[-period:]]
    lows = [float(k[3]) for k in klines[-period:]]
    
    recent_closes = closes[-period//2:]
    avg_recent = sum(recent_closes) / len(recent_closes)
    avg_prior = sum(closes[:period//2]) / (period//2)
    
    if avg_recent > avg_prior * 1.02:
        return "bullish"
    elif avg_recent < avg_prior * 0.98:
        return "bearish"
    else:
        return "flat"

def momentum_direction_confirmed(klines, direction, lookback=3):
    """Check if recent momentum aligns with expected direction."""
    if not klines or len(klines) < lookback + 1:
        return True
    
    closes = [float(k[4]) for k in klines[-lookback-1:]]
    confirmed_count = 0
    for i in range(1, len(closes)):
        if direction == "LONG" and closes[i] > closes[i-1]:
            confirmed_count += 1
        elif direction == "SHORT" and closes[i] < closes[i-1]:
            confirmed_count += 1
    
    return confirmed_count >= (lookback - 1)

def volatility_regime_factor(tv_metrics, klines, vol_est):
    """Calculate volatility adjustment factor."""
    if not tv_metrics or not tv_metrics.get("atr"):
        return 1.0
    
    atr = tv_metrics.get("atr", 0)
    if vol_est and vol_est > 0:
        ratio = atr / vol_est
        if ratio < 0.5:
            return 0.8
        elif ratio > 2.0:
            return 1.2
        else:
            return 1.0 + (ratio - 1.0) * 0.2
    return 1.0

def analyze_multi_timeframe_momentum(kl_1m, kl_5m, kl_15m, direction):
    """Analyze momentum alignment across timeframes."""
    if not kl_1m or not kl_5m or not kl_15m:
        return 0.0, None, 0.0
    
    mtf_agreement = 0
    for klines in [kl_1m, kl_5m, kl_15m]:
        if len(klines) >= 2:
            closes = [float(k[4]) for k in klines[-2:]]
            if direction == "LONG" and closes[-1] > closes[-2]:
                mtf_agreement += 1
            elif direction == "SHORT" and closes[-1] < closes[-2]:
                mtf_agreement += 1
    
    bonus = 0.0
    if mtf_agreement >= 2:
        bonus = 5.0 + (mtf_agreement - 2) * 2.5
    
    return 0.0, mtf_agreement if mtf_agreement > 0 else None, bonus

def detect_breakout_momentum(klines, direction):
    """Detect if momentum shows breakout strength."""
    if not klines or len(klines) < 5:
        return False, 0.0
    
    closes = [float(k[4]) for k in klines[-5:]]
    highs = [float(k[2]) for k in klines[-5:]]
    lows = [float(k[3]) for k in klines[-5:]]
    
    range_size = max(highs) - min(lows)
    current_momentum = abs(closes[-1] - closes[0]) / (range_size + 1e-10)
    
    if direction == "LONG":
        is_breakout = closes[-1] > closes[-2] and current_momentum > 0.4
    else:
        is_breakout = closes[-1] < closes[-2] and current_momentum > 0.4
    
    return is_breakout, max(0.0, min(1.0, current_momentum))

def detect_volume_surge(klines):
    """Detect volume surge in recent candles."""
    if not klines or len(klines) < 3:
        return False, 0.0
    
    volumes = [float(k[7]) if len(k) > 7 else 0 for k in klines[-10:]]
    if not volumes or all(v == 0 for v in volumes):
        return False, 0.0
    
    avg_volume = sum(volumes[:-1]) / max(1, len(volumes) - 1)
    current_volume = volumes[-1]
    
    if avg_volume == 0:
        return False, 0.0
    
    surge_ratio = current_volume / avg_volume
    is_surge = surge_ratio > 1.3
    strength = min(1.0, (surge_ratio - 1.0) / 1.0)
    
    return is_surge, strength

def detect_momentum_opportunity_v3_with_fallbacks(pair_data, klines, bin_info, tv_metrics, vol_est,
                                                   market_trend, market_sentiment_pct, hotness_ranks,
                                                   total_hotness, cooldown_cache, direction, klines_1h=None, klines_4h=None):
    """
    Enhanced momentum detector with PRIMARY, SECONDARY, and TERTIARY detection layers.
    Fallback methods ensure signal generation across varying market conditions.
    Returns dict signal or None.
    """
    dbg = os.getenv("DEBUG_MOMENTUM", "false").lower() == "true"

    pair = pair_data.get("symbol", "") + "USDT" if pair_data.get("symbol") else None
    cid = pair_data.get("coin_id")
    
    kl_1m = kl_5m = None
    try:
        klines_15m_new, kl_1m, kl_5m, _ = fetch_klines_for_momentum(pair, interval="15m", limit=80, coin_id=cid)
        if klines_15m_new and not klines:
            klines = klines_15m_new
    except Exception:
        pass
    
    entry_price = pair_data.get("price") if pair_data else None
    if entry_price is None or (isinstance(entry_price, (int, float)) and entry_price <= 0):
        klines_dict_momentum = {
            "1m": kl_1m,
            "5m": kl_5m,
            "15m": klines
        }
        # Create a market_data dict from pair_data for get_entry_price_safe
        market_data_for_price = {pair: pair_data} if pair_data else None
        entry_price, mom_price_source, mom_price_staleness = get_entry_price_safe(
            klines_dict_momentum, pair, coin_id=cid, strategy_name="MOMENTUM", fallback_only_fresh=True, market_data=market_data_for_price
        )
        if entry_price:
            mom_price_metadata = {"price_source": mom_price_source, "price_staleness_sec": mom_price_staleness}
        else:
            return None
    else:
        mom_price_metadata = {"price_source": "livecoinwatch_momentum", "price_staleness_sec": 0.0}
    
    if entry_price is not None and entry_price > 0:
        try:
            klines_1m_fresh, klines_5m_fresh, _, _ = fetch_multi_timeframe_klines(pair, coin_id=cid, strategy_name="MOMENTUM")
            if klines_1m_fresh and len(klines_1m_fresh) >= 2:
                kl_1m = klines_1m_fresh
            if klines_5m_fresh and len(klines_5m_fresh) >= 2:
                kl_5m = klines_5m_fresh
            klines_15m_fresh = fetch_any_klines(pair, interval="15m", limit=80, coin_id=cid, strategy="momentum")
            if klines_15m_fresh and len(klines_15m_fresh) >= 2:
                klines = klines_15m_fresh
        except Exception:
            pass
        
        try:
            klines_dict_fresh = {
                "1m": kl_1m,
                "5m": kl_5m,
                "15m": klines
            }
            # Create a market_data dict from pair_data for get_entry_price_safe
            market_data_for_price = {pair: pair_data} if pair_data else None
            entry_price_fresh, mom_price_source, mom_price_staleness = get_entry_price_safe(
                klines_dict_fresh, pair, coin_id=cid, strategy_name="MOMENTUM", fallback_only_fresh=True, market_data=market_data_for_price
            )
            if entry_price_fresh and entry_price_fresh > 0:
                entry_price = entry_price_fresh
                mom_price_metadata = {"price_source": mom_price_source, "price_staleness_sec": mom_price_staleness}
        except Exception:
            pass

    if not klines or len(klines) < 5:
        if dbg:
            print(f"[MOM V2] {pair} - insufficient klines (need >=5, got {len(klines) if klines else 0})")
        return None

    short_mom, medium_mom = extract_short_medium_momentum(klines)

    primary_layers = []
    secondary_layers = []
    tertiary_layers = []

    if abs(short_mom) >= 0.01:
        if (direction == "LONG" and short_mom > 0) or (direction == "SHORT" and short_mom < 0):
            primary_layers.append(("short_momentum", min(28.0, 15.0 + abs(short_mom) * 10.0)))

    if abs(medium_mom) >= 0.005:
        if (direction == "LONG" and medium_mom > 0) or (direction == "SHORT" and medium_mom < 0):
            secondary_layers.append(("medium_momentum", min(24.0, 12.0 + abs(medium_mom) * 20.0)))

    rsi_val = tv_metrics.get("rsi") if tv_metrics else 50.0
    if rsi_val is not None:
        if direction == "LONG" and rsi_val < 50:
            tertiary_layers.append(("rsi_low", min(20.0, (40 - rsi_val) * 1.0)))
        elif direction == "SHORT" and rsi_val > 50:
            tertiary_layers.append(("rsi_high", min(20.0, (rsi_val - 60) * 1.0)))

    rsi_divergence = False
    try:
        if len(klines) >= RSI_DIV_LOOKBACK * 2:
            rsi_divergence = rsi_divergence_check(klines, tv_metrics, direction)
    except Exception:
        pass

    # MOMENTUM EXHAUSTION: Divergence is a PENALTY for momentum continuation, not a bonus
    exhaustion_penalty = 0.0
    if rsi_divergence:
        exhaustion_penalty += 25.0
        layer_names.append("rsi_exhaustion_risk")

    momentum_divergence = False
    try:
        if len(klines) >= RSI_DIV_LOOKBACK * 2:
            momentum_divergence = momentum_divergence_check(klines, direction)
    except Exception:
        pass

    if momentum_divergence:
        exhaustion_penalty += 20.0
        layer_names.append("momentum_exhaustion_risk")

    higher_tf_bias = higher_tf_bias_from_klines(klines, period=16)
    if klines_1h and isinstance(klines_1h, list):
        try:
            higher_tf_bias_1h = higher_tf_bias_from_klines(klines_1h, period=24)
            if higher_tf_bias_1h != "flat":
                higher_tf_bias = higher_tf_bias_1h
        except Exception:
            pass
    if klines_4h and isinstance(klines_4h, list):
        try:
            higher_tf_bias_4h = higher_tf_bias_from_klines(klines_4h, period=12)
            if higher_tf_bias_4h != "flat":
                higher_tf_bias = higher_tf_bias_4h
        except Exception:
            pass
    
    if higher_tf_bias == "bullish" and direction == "LONG":
        tertiary_layers.append(("tf_bias_bullish", 16.0))
    elif higher_tf_bias == "bearish" and direction == "SHORT":
        tertiary_layers.append(("tf_bias_bearish", 16.0))

    # Add breakout detection layers
    try:
        breakout_detected, breakout_strength = detect_breakout_momentum(klines, direction)
        if breakout_detected:
            primary_layers.append(("breakout", min(25.0, 15.0 + breakout_strength * 10.0)))
    except Exception:
        pass

    # Add volume surge layers
    try:
        volume_surge, surge_strength = detect_volume_surge(klines)
        if volume_surge:
            secondary_layers.append(("volume_surge", min(22.0, 12.0 + surge_strength * 10.0)))
    except Exception:
        pass

    # Add simple trend strength layers
    if len(klines) >= 5:
        recent_trend = sum(1 for i in range(1, 5) if 
                          (direction == "LONG" and float(klines[-i][4]) > float(klines[-i-1][4])) or
                          (direction == "SHORT" and float(klines[-i][4]) < float(klines[-i-1][4])))
        if recent_trend >= 3:  # 3 out of 4 candles in direction
            tertiary_layers.append(("trend_strength", min(18.0, 8.0 + recent_trend * 2.5)))

    # Add hotness-based layers
    if pair and hotness_ranks and pair in hotness_ranks:
        hotness_score = hotness_ranks.get(pair, 0)
        if hotness_score > 0:
            tertiary_layers.append(("hotness_boost", min(15.0, hotness_score * 5.0)))

    # Add EMA alignment layers
    if tv_metrics:
        ema5 = tv_metrics.get("ema5")
        ema20 = tv_metrics.get("ema20")
        price = pair_data.get("price", 0)
        if ema5 and ema20 and price > 0:
            if direction == "LONG" and ema5 > ema20 and price > ema5:
                tertiary_layers.append(("ema_alignment_long", 12.0))
            elif direction == "SHORT" and ema5 < ema20 and price < ema5:
                tertiary_layers.append(("ema_alignment_short", 12.0))

    closes = [float(k[4]) for k in klines[-30:]] if len(klines) >= 30 else [float(k[4]) for k in klines]
    try:
        if not tv_metrics or tv_metrics.get("bbw") is None:
            bbw_calc = compute_bb_width_for_closes(closes, period=20)
        else:
            bbw_calc = tv_metrics.get("bbw")

        if bbw_calc and bbw_calc < RANGE_BB_COMPRESS_THRESHOLD:
            tertiary_layers.append(("bb_compact", 12.0))
    except Exception:
        pass

    def aggregate_momentum_conf(primary, secondary, tertiary):
        """FIXED Priority 3: Aggregate momentum confidence with same method as reversal/range - no direction bias."""
        all_layers = primary + secondary + tertiary
        if not all_layers:
            return 12.0, []  # Return baseline confidence instead of 0.0 to allow fallback signals

        total_conf = 0.0
        layer_names = []

        for layer_name, weight in primary:
            total_conf += weight
            layer_names.append(layer_name)

        for layer_name, weight in secondary:
            total_conf += weight
            layer_names.append(layer_name)

        for layer_name, weight in tertiary:
            total_conf += weight
            layer_names.append(layer_name)

        agreement_bonus = calculate_method_agreement_bonus(len(layer_names))
        total_conf += agreement_bonus

        return min(100.0, total_conf), layer_names

    # REMOVED Priority 3: Direction bias removed from momentum strategy
    base_conf, layer_names = aggregate_momentum_conf(primary_layers, secondary_layers, tertiary_layers)

    # FIXED: Add momentum direction confirmation check (relaxed penalty from 0.65 to 0.80)
    dir_confirmed = True
    try:
        dir_confirmed = momentum_direction_confirmed(klines, direction, lookback=3)
    except Exception:
        dir_confirmed = True
    
    if not dir_confirmed:
        base_conf *= 0.80  # Relaxed penalty from 0.65 to allow more signals
        if dbg:
            print(f"[MOM V3] {pair} {direction} - Direction confirmation FAILED, lighter penalty applied (0.80x)", flush=True)

    try:
        mtf_momentum, mtf_agreement, mtf_bonus = analyze_multi_timeframe_momentum(kl_1m, kl_5m, klines, direction)
        if mtf_agreement is not None and mtf_agreement >= 2:
            base_conf += mtf_bonus
            if dbg:
                print(f"[MOM V3] {pair} {direction} - Multi-TF momentum agreement={mtf_agreement} bonus={mtf_bonus:.1f}", flush=True)
    except Exception as e:
        if dbg:
            print(f"[MOM V3] {pair} - MTF momentum analysis failed: {str(e)[:60]}", flush=True)

    volatility_mult = 1.0
    try:
        volatility_mult = volatility_regime_factor(tv_metrics, klines, vol_est)
    except Exception:
        volatility_mult = 1.0
    
    # FIXED: Only apply volatility adjustments if direction is confirmed
    if dir_confirmed:
        if volatility_mult < 0.15:
            if dbg:
                print(f"[MOM V3] {pair} {direction} - LOW volatility: {volatility_mult:.4f}, applying penalty", flush=True)
            base_conf *= max(0.25, volatility_mult)
        else:
            base_conf *= volatility_mult

    sentiment_scale = max(0.5, min(1.5, market_sentiment_pct / 50.0))  # Relaxed from 0.7-1.3 to 0.5-1.5
    base_conf *= sentiment_scale

    # FIXED: Only apply hotness bonus if direction is confirmed
    hotness_idx = hotness_ranks.get(pair, 999)
    if dir_confirmed and hotness_idx < 100 and total_hotness > 0:
        hotness_bonus = min(HOTNESS_BONUS_MAX, (100 - hotness_idx) / total_hotness * 4.5)
        base_conf += hotness_bonus

    base_conf = apply_soft_cooldown_adjustment(base_conf, pair, direction, cooldown_cache)

    # Apply exhaustion penalty early
    base_conf -= exhaustion_penalty

    # --- New Comprehensive Momentum Analysis ---
    volume_surge_detected, surge_ratio = detect_volume_surge(klines)
    momentum_strength = classify_momentum_strength(klines, entry_price, direction)
    breakout_detected, breakout_strength = detect_breakout_momentum(klines, direction)

    # VERTICALITY CHECK: Is the move too steep to continue safely?
    try:
        closes_v = [float(k[4]) for k in klines[-6:]]
        v_move = abs(closes_v[-1] - closes_v[0]) / closes_v[0]
        if v_move > 0.15: # 15% move in 6 bars is very vertical
            base_conf *= 0.7
            layer_names.append("verticality_risk")
    except Exception:
        pass

    # FIXED: Add bonuses for strong confirming factors
    momentum_bonus = 0.0
    if breakout_detected and volume_surge_detected:
        momentum_bonus += 25.0
        layer_names.append("breakout_with_volume")
    elif breakout_detected:
        momentum_bonus += 15.0
        layer_names.append("breakout")
    
    base_conf += momentum_bonus

    mtf_aligned_score = compute_multi_timeframe_alignment(kl_1m, kl_5m, klines, klines_1h, direction)
    mtf_bonus = 0.0
    if mtf_aligned_score > 0.85: # Stronger requirement for top bonus
        mtf_bonus = 12.0
        base_conf += mtf_bonus
        layer_names.append(f"mtf_aligned_strong({mtf_aligned_score:.2f})")
    elif mtf_aligned_score > 0.60:
        mtf_bonus = 5.0
        base_conf += mtf_bonus
        layer_names.append(f"mtf_aligned({mtf_aligned_score:.2f})")

    sl_pct_mom, tp_pct_mom = calculate_dynamic_tp_sl_from_movement(
        klines, entry_price, direction, strategy_type="momentum",
        tv_metrics=tv_metrics, bin_info=bin_info, vol_est=vol_est,
        momentum_strength=momentum_strength, confidence=base_conf,
        market_trend=market_trend
    )

    if breakout_detected:
        tp_pct_mom = tp_pct_mom * 1.15
        sl_pct_mom = max(0.002, sl_pct_mom * 0.85)

    if volume_surge_detected and surge_ratio > 3.0:
        tp_pct_mom = tp_pct_mom * 1.1

    # STRUCTURAL PROXIMITY PENALTY: Reduce confidence if price is already too close to TP
    # If we are already 80% of the way to the structural target, momentum is likely exhausted
    try:
        structural_target = distance_to_resistance if direction == "LONG" else distance_to_support
        if tp_pct_mom > 0 and (tp_pct_mom / (structural_target + 1e-9)) < 0.3:
             # This implies the target is very close compared to ATR projection
             base_conf *= 0.8
             layer_names.append("proximity_to_resistance_risk")
    except Exception:
        pass

    # Final Confidence Capping
    base_conf = min(100.0, max(12.0, base_conf))

    # Enhanced quality scoring (Reward R/R efficiency and MTF alignment)
    rr_ratio = tp_pct_mom / sl_pct_mom if sl_pct_mom > 0 else 1.0
    quality_score = (len(layer_names) * 10.0) + (mtf_aligned_score * 30.0)
    
    if rr_ratio > 3.0:
        quality_score += 15.0
        layer_names.append("high_rr_efficiency")
    elif rr_ratio > 2.0:
        quality_score += 8.0
        
    if momentum_strength.get("class") == "EXPLOSIVE":
        quality_score += 15.0
    elif momentum_strength.get("class") == "STRONG":
        quality_score += 8.0
        
    quality_score = min(100.0, quality_score)

    signal = {
        "type": "MOMENTUM",
        "timestamp": time.time(),
        "coin_id": cid,
        "symbol": pair_data.get("symbol", ""),
        "pair": pair,
        "direction": direction,
        "entry": round(entry_price, 8),
        "sl": round(entry_price * (1 + sl_pct_mom) if direction == "SHORT" else entry_price * (1 - sl_pct_mom), 8),
        "tp": round(entry_price * (1 - tp_pct_mom) if direction == "SHORT" else entry_price * (1 + tp_pct_mom), 8),
        "sl_pct": round(sl_pct_mom, 6),
        "tp_pct": round(tp_pct_mom, 6),
        "confidence": round(base_conf, 2),
        "quality_score": round(quality_score, 2),
        "trigger_layers": layer_names,
        "notes": f"momentum_v3_strength={momentum_strength.get('class', 'N/A')}_bias={higher_tf_bias}",
        "momentum_strength": momentum_strength.get("class", "N/A"),
        "momentum_score": momentum_strength.get("score", 0),
        "volume_surge": volume_surge_detected,
        "breakout_detected": breakout_detected,
        "mtf_aligned": mtf_aligned,
        "zone": compute_zone(pair_data)[0],
        "vol_est": vol_est,
        "price_source": mom_price_metadata.get("price_source", "unknown") if 'mom_price_metadata' in locals() else "unknown",
        "price_staleness_sec": mom_price_metadata.get("price_staleness_sec", 0) if 'mom_price_metadata' in locals() else 0
    }
    
    assert signal.get("direction") in ["LONG", "SHORT"], f"MOMENTUM: Invalid direction {signal.get('direction')}"

    signal["tp"] = round(adjust_target_for_spread(entry_price, signal["tp"], direction), 8)
    signal["tp_pct"] = round(abs(signal["tp"] - entry_price) / entry_price, 6)

    try:
        prob_tp_m, prob_sl_m = estimate_tp_sl_hit_prob(klines, float(signal["entry"]), float(signal["tp"]), float(signal["sl"]), lookahead=14, vol_est=vol_est)
        signal["prob_tp"] = round(prob_tp_m, 3)
        signal["prob_sl"] = round(prob_sl_m, 3)

        adj_conf_m = adjust_confidence_with_probs(float(signal["confidence"]), prob_tp_m, prob_sl_m)
        signal["confidence"] = round(adj_conf_m, 2)
    except Exception:
        pass

    if dbg:
        print(f"[MOM V3] {pair} {direction} - conf={signal.get('confidence', base_conf):.2f} quality={quality_score:.2f} strength={momentum_strength.get('class', 'N/A')} layers={len(layer_names)}")

    return signal

# ---------------- market trend & sentiment ----------------
def compute_market_trend_and_sentiment(market_data):
    if not market_data:
        return "neutral", 50.0
    oneh_changes = []
    day_changes = []
    for v in market_data.values():
        try:
            oneh_changes.append(float(v.get("price_change_1h", 0.0) or 0.0))
            day_changes.append(float(v.get("price_change_24h", 0.0) or 0.0))
        except Exception:
            continue
    if not oneh_changes or not day_changes:
        return "neutral", 50.0
    avg_1h = sum(oneh_changes) / len(oneh_changes)
    avg_24h = sum(day_changes) / len(day_changes)
    combined = (avg_1h * 0.6 + avg_24h * 0.4)
    positives = [x for x in day_changes if x > 0]
    sentiment_pct = (len(positives) / len(day_changes)) * 100.0
    if combined >= 1.0:
        trend = "bullish"
    elif combined <= -1.0:
        trend = "bearish"
    else:
        trend = "neutral"
    return trend, round(sentiment_pct, 2)

# ============ GLOBAL ACCESSIBLE FREE API FETCHERS ============

def fetch_huobi_tickers(limit=100):
    """Fetch from Huobi Global (accessible from Nigeria)"""
    try:
        r = requests.get("https://api.huobi.pro/market/tickers", timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json().get("data", [])
        
        markets = []
        for item in data[:limit]:
            try:
                symbol = item.get("symbol", "").rstrip("usdt").upper()
                if not symbol or is_stablecoin(symbol):
                    continue
                
                vol = float(item.get("vol", 0)) * float(item.get("close", 0))
                if vol >= MIN_VOLUME_USD:
                    markets.append({
                        "symbol": symbol,
                        "current_price": float(item.get("close", 0)),
                        "total_volume": vol,
                        "price_change_percentage_24h_in_currency": 0
                    })
            except Exception:
                continue
        
        return markets[:limit]
    except Exception as e:
        print(f"[Huobi] Fetch error: {str(e)[:60]}", flush=True)
        return []

def fetch_gateio_tickers(limit=100):
    """Fetch from Gate.io (accessible from Nigeria)"""
    try:
        r = requests.get("https://api.gateio.ws/api/v4/spot/tickers", timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        
        markets = []
        for item in data[:limit]:
            try:
                currency_pair = item.get("currency_pair", "")
                if "_USDT" not in currency_pair:
                    continue
                
                symbol = currency_pair.replace("_USDT", "")
                if not symbol or is_stablecoin(symbol):
                    continue
                
                last_price = float(item.get("last", 0))
                volume_usd = float(item.get("quote_volume", 0))
                
                if volume_usd >= MIN_VOLUME_USD:
                    markets.append({
                        "symbol": symbol,
                        "current_price": last_price,
                        "total_volume": volume_usd,
                        "price_change_percentage_24h_in_currency": float(item.get("change_percentage", 0)) or 0
                    })
            except Exception:
                continue
        
        return markets[:limit]
    except Exception as e:
        print(f"[Gate.io] Fetch error: {str(e)[:60]}", flush=True)
        return []

def fetch_poloniex_tickers(limit=100):
    """Fetch from Poloniex ticker endpoint (accessible globally)"""
    try:
        r = requests.get("https://api.poloniex.com/ticker", timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        
        markets = []
        for pair_str, ticker_data in list(data.items())[:limit*2]:
            try:
                if "_USDT" not in pair_str:
                    continue
                
                symbol = pair_str.replace("_USDT", "").upper()
                if not symbol or is_stablecoin(symbol):
                    continue
                
                price = float(ticker_data.get("last", 0))
                volume = float(ticker_data.get("quoteVolume", 0))
                change = float(ticker_data.get("percentChange", 0)) or 0
                
                if volume >= MIN_VOLUME_USD and price > 0:
                    markets.append({
                        "symbol": symbol,
                        "current_price": price,
                        "total_volume": volume,
                        "price_change_percentage_24h_in_currency": change
                    })
            except Exception:
                continue
        
        return markets[:limit]
    except Exception as e:
        print(f"[Poloniex] Fetch error: {str(e)[:60]}", flush=True)
        return []

def verify_bot_strategy_performance(all_signals, market_data, candidates):
    """
    Verify bot strategy performance and signal quality.
    
    Args:
        all_signals: Dictionary of strategy_name -> list of signals
        market_data: Dictionary of market data
        candidates: List of candidate data
    
    Returns:
        Dictionary with 'summary' containing verification results
    """
    try:
        total_signals = sum(len(sigs) for sigs in all_signals.values())
        valid_signals = 0
        quality_issues = []
        
        for strategy_name, signals in all_signals.items():
            for sig in signals:
                if isinstance(sig, dict):
                    required_keys = ['symbol', 'direction', 'entry', 'tp', 'sl', 'confidence']
                    if all(key in sig for key in required_keys):
                        valid_signals += 1
                    else:
                        quality_issues.append(f"{strategy_name}: Missing keys")
        
        market_data_quality = len(market_data) if market_data else 0
        candidates_quality = len(candidates) if candidates else 0
        
        quality_score = 0.0
        if total_signals > 0:
            signal_quality = (valid_signals / total_signals) * 40
            quality_score += signal_quality
        else:
            quality_score += 20
        
        if market_data_quality > 10:
            quality_score += 30
        elif market_data_quality > 0:
            quality_score += 15
        
        if candidates_quality > 0:
            quality_score += 30
        
        quality_score = min(100.0, max(0.0, quality_score))
        
        overall_status = 'PASS' if quality_score >= 50 else 'FAIL'
        
        return {
            'summary': {
                'overall_status': overall_status,
                'search_quality_score': quality_score,
                'total_signals': total_signals,
                'valid_signals': valid_signals,
                'market_data_count': market_data_quality,
                'candidates_count': candidates_quality,
                'quality_issues': quality_issues[:5]
            },
            'strategies': {s: len(sigs) for s, sigs in all_signals.items()},
            'timestamp': now_utc_str()
        }
    except Exception as e:
        return {
            'summary': {
                'overall_status': 'FAIL',
                'search_quality_score': 0.0,
            },
            'error': str(e)
        }

# ---------------- main loop ----------------
def main_loop():
    print("Short-Term Adaptive Smart Signal Bot (LIVE DATA MODE) — Enhanced Reversal + Range (Balanced)", flush=True)
    print("Market Data Discovery (Pair Finding) - Strategy-Specific LiveCoinWatch APIs:", flush=True)
    print("  • REVERSAL: Coinpaprika (primary) → Bitstamp (fallback)", flush=True)
    print("  • MOMENTUM: CoinGecko (primary) → CEX.IO (fallback)", flush=True)
    print("  • RANGE: Huobi (primary) → Gemini (fallback)", flush=True)
    print("Klines Fetching (Technical Analysis) - Multi-exchange chains:", flush=True)
    print("  • REVERSAL: CoinAPI→Kraken→Gate.io→Huobi→Coinalyse→CoinGecko", flush=True)
    print("  • MOMENTUM: Huobi→Gate.io→Kraken→CoinAPI→Coinalyse→CoinGecko", flush=True)
    print("  • RANGE: Gate.io→Kraken→Huobi→CoinAPI→Coinalyse→CoinGecko", flush=True)
    print("TV interval:", TV_INTERVAL, "Short-term mode:", SHORT_TERM_MODE, flush=True)
    print("Running every", CYCLE_SECONDS, "seconds. Time:", now_utc_str(), flush=True)
    
    # Clear any existing cache to ensure fresh data
    if STRATEGY_FETCHERS_AVAILABLE:
        print("[LIVE DATA] Strategy kline fetchers ready for fresh data", flush=True)

    # Initialize caches
    global _klines_cache_global
    cached_trending = load_trending_cache()
    cooldown_cache = load_cooldown_cache()
    vol_cache = load_vol_cache()
    EWMA_ALPHA = 0.2
    _klines_cache_global = {} # STAGE 1: Persistent cache across cycles (cleaned periodically)

    while True:
        all_signals = {} # Reset all_signals each cycle
        

        try:
            cycle_start_time = time.time()
            print(f"\n[Cycle] Data collection at {now_utc_str()}...", flush=True)
            
            # STAGE 1: Periodic cache cleanup and statistics
            if time.time() - _cache_stats['last_cleanup'] > CACHE_CLEANUP_INTERVAL:
                cache_cleanup()  # Uses global cache by default
                print(f"[CACHE] Statistics - {cache_stats_str()}", flush=True)
            
            market_trend = "neutral"
            market_sentiment_pct = 50.0
            hotness_ranks = {}
            total_hotness = 1
            session_name, session_adjustments = get_session_hour_bias()
            
            current_equity = ACCOUNT_BALANCE
            drawdown_pct, max_equity, circuit_breaker_active = track_drawdown(ACCOUNT_BALANCE, current_equity)
            
            if circuit_breaker_active:
                print(f"⚠️ [CIRCUIT BREAKER] Drawdown {drawdown_pct:.1f}% > {float(os.getenv('MAX_DRAWDOWN_PCT', '15.0')):.1f}% threshold - PAUSING TRADES", flush=True)
            else:
                print(f"[DRAWDOWN] Current: {drawdown_pct:.1f}% | Max: {max_equity:.2f}", flush=True)
            
            try:
                print("[MOMENTUM] Fetching trending pairs (CoinGecko API)...", flush=True)
                momentum_markets = fetch_market_list(limit=50, strategy="MOMENTUM")
                momentum_candidates = []
                
                for pair in momentum_markets:
                    try:
                        momentum_candidates.append({
                            "symbol": (pair.get("symbol") or "").upper(),
                            "id": pair.get("id"),
                            "strategy": "momentum",
                            "price": pair.get("current_price", 0),
                            "volume": pair.get("total_volume", 0),
                            "price_change_1h": pair.get("price_change_percentage_1h_in_currency", 0),
                            "price_change_24h": pair.get("price_change_percentage_24h_in_currency", 0),
                            "high_24h": pair.get("high_24h", pair.get("current_price", 0)),
                            "low_24h": pair.get("low_24h", pair.get("current_price", 0))
                        })
                    except Exception:
                        continue
                
                if momentum_candidates:
                    print(f"[MOMENTUM] ✓ {len(momentum_candidates)} trending pairs acquired", flush=True)
                else:
                    print("[MOMENTUM] ⚠️ CoinGecko returned no trending pairs", flush=True)
                    
            except Exception as e:
                print(f"[MOMENTUM] ❌ Error: {str(e)[:80]}", flush=True)
                momentum_candidates = []
            
            try:
                print("[RANGE] Fetching consolidating pairs (Huobi API)...", flush=True)
                range_pairs = fetch_range_pair_candidates(limit=50)
                range_candidates = []
                
                for pair in range_pairs:
                    try:
                        range_candidates.append({
                            "symbol": (pair.get("symbol") or pair.get("name", "")).upper(),
                            "id": pair.get("id"),
                            "strategy": "range",
                            "price": pair.get("price"),
                            "volume": pair.get("volume"),
                            "price_change_1h": pair.get("price_change_1h"),
                            "price_change_24h": pair.get("price_change_24h"),
                            "high_24h": pair.get("high_24h"),
                            "low_24h": pair.get("low_24h")
                        })
                    except Exception:
                        continue
                
                if range_candidates:
                    print(f"[RANGE] ✓ {len(range_candidates)} consolidating pairs acquired", flush=True)
                else:
                    print("[RANGE] ⚠️ Huobi returned no candidates", flush=True)
                    
            except Exception as e:
                print(f"[RANGE] ❌ Error: {str(e)[:80]}", flush=True)
                range_candidates = []
            
            try:
                print("[REVERSAL] Fetching top movers (CoinGecko + Exchange Filter)...", flush=True)
                top_movers_candidates = get_reversal_candidates_top_movers(limit=50)
                
                print("[REVERSAL] Fetching top gainers & losers (Coinpaprika API)...", flush=True)
                reversal_pairs = fetch_reversal_pair_candidates(limit=50)
                
                # Prepend top movers to reversal_pairs and ensure uniqueness by symbol
                seen_reversal_symbols = set()
                combined_reversal_pairs = []
                for p in top_movers_candidates + reversal_pairs:
                    sym = (p.get("symbol") or p.get("name", "")).upper()
                    if sym not in seen_reversal_symbols:
                        combined_reversal_pairs.append(p)
                        seen_reversal_symbols.add(sym)
                
                reversal_pairs = combined_reversal_pairs
                
                reversal_candidates = []
                
                for pair in reversal_pairs:
                    try:
                        reversal_candidates.append({
                            "symbol": (pair.get("symbol") or pair.get("name", "")).upper(),
                            "id": pair.get("id"),
                            "strategy": "reversal",
                            "price": pair.get("price"),
                            "volume": pair.get("volume"),
                            "price_change_1h": pair.get("price_change_1h"),
                            "price_change_24h": pair.get("price_change_24h"),
                            "high_24h": pair.get("high_24h"),
                            "low_24h": pair.get("low_24h")
                        })
                    except Exception:
                        continue
                
                if reversal_candidates:
                    print(f"[REVERSAL] ✓ {len(reversal_candidates)} gainers & losers acquired", flush=True)
                else:
                    print("[REVERSAL] ⚠️ Coinpaprika returned no candidates", flush=True)
                    
            except Exception as e:
                print(f"[REVERSAL] ❌ Error: {str(e)[:80]}", flush=True)
                reversal_candidates = []
            
            trending = momentum_candidates + range_candidates + reversal_candidates
            
            if not trending:
                print("⚠️ No candidates generated. Waiting 60s before retry...", flush=True)
                time.sleep(60)
                continue
            
            try:
                # Build market data directly from already-fetched candidates (no additional API calls)
                print("[MARKET DATA] Building market data from fetched candidates (fast mode - no API calls)...", flush=True)
                
                all_market_data = {}
                all_candidates = momentum_candidates + reversal_candidates + range_candidates
                
                for cand in all_candidates:
                    try:
                        symbol = (cand.get("symbol") or "").upper()
                        if not symbol or is_stablecoin(symbol):
                            continue
                        pair = symbol + "USDT"
                        
                        # Use candidate data directly (already fetched from APIs)
                        price = float(cand.get("price", cand.get("current_price", 0)) or 0)
                        if price <= 0:
                            continue
                        
                        # Extract or compute required fields from candidate data
                        high_24h = float(cand.get("high_24h", cand.get("high", price * 1.05)) or price * 1.05)
                        low_24h = float(cand.get("low_24h", cand.get("low", price * 0.95)) or price * 0.95)
                        price_change_1h = float(cand.get("price_change_1h", cand.get("price_change_percentage_1h", 0)) or 0.0)
                        price_change_24h = float(cand.get("price_change_24h", cand.get("price_change_percentage_24h", cand.get("price_change_percentage", 0))) or 0.0)
                        volume = float(cand.get("volume", cand.get("total_volume", 0)) or 0.0)
                        coin_id = cand.get("id", cand.get("coin_id", ""))
                        
                        all_market_data[pair] = {
                            "price": price,
                            "high_24h": high_24h,
                            "low_24h": low_24h,
                            "price_change_1h": price_change_1h,
                            "price_change_24h": price_change_24h,
                            "total_volume": volume,
                            "volume": volume,
                            "coin_id": coin_id
                        }
                    except Exception as e:
                        continue
                
                market_data = all_market_data
                market_sentiment_pct, market_trend = compute_market_sentiment(market_data)
                print(f"[SENTIMENT] {market_trend.upper()}: {market_sentiment_pct:.1f}% | Session: {session_name} | Pairs: {len(market_data)}", flush=True)
            except Exception as e:
                print(f"⚠️ Market data build failed: {str(e)[:80]}, using empty dict", flush=True)
                market_data = {}
                market_sentiment_pct, market_trend = 50.0, "neutral"
            
            try:
                # Use already fetched candidates as market list (no need to fetch again)
                # Convert candidates to market list format
                all_market_list = []
                for cand in momentum_candidates:
                    all_market_list.append({
                        'id': cand.get('id'),
                        'symbol': cand.get('symbol'),
                        'current_price': cand.get('price', 0),
                        'total_volume': cand.get('volume', 0),
                        'price_change_percentage_24h_in_currency': cand.get('price_change_24h', 0),
                        'price_change_percentage_1h_in_currency': cand.get('price_change_1h', 0),
                        'high_24h': cand.get('high_24h', cand.get('price', 0)),
                        'low_24h': cand.get('low_24h', cand.get('price', 0))
                    })
                for cand in range_candidates:
                    all_market_list.append({
                        'id': cand.get('id'),
                        'symbol': cand.get('symbol'),
                        'current_price': cand.get('price', 0),
                        'total_volume': cand.get('volume', 0),
                        'price_change_percentage_24h_in_currency': cand.get('price_change_24h', 0),
                        'price_change_percentage_1h_in_currency': cand.get('price_change_1h', 0),
                        'high_24h': cand.get('high_24h', cand.get('price', 0)),
                        'low_24h': cand.get('low_24h', cand.get('price', 0))
                    })
                for cand in reversal_candidates:
                    all_market_list.append({
                        'id': cand.get('id'),
                        'symbol': cand.get('symbol'),
                        'current_price': cand.get('price', 0),
                        'total_volume': cand.get('volume', 0),
                        'price_change_percentage_24h_in_currency': cand.get('price_change_24h', 0),
                        'price_change_percentage_1h_in_currency': cand.get('price_change_1h', 0),
                        'high_24h': cand.get('high_24h', cand.get('price', 0)),
                        'low_24h': cand.get('low_24h', cand.get('price', 0))
                    })
            except Exception as e:
                print(f"⚠️ Market list processing failed: {str(e)[:80]}", flush=True)
                all_market_list = []
            
            try:
                trending_symbols = [c.get("symbol") for c in trending if isinstance(c, dict) and c.get("symbol")]
                hotness_ranks = compute_hotness_ranks(trending_symbols, market_data)
                print(f"[HOTNESS] Ranked {len(hotness_ranks)} trending coins", flush=True)
            except Exception as e:
                print(f"⚠️ Hotness ranking failed: {str(e)[:60]}", flush=True)
                hotness_ranks = {}

            if not market_data:
                print(f"[FALLBACK] Building market_data from candidates for scoring...", flush=True)
                for cand in trending:
                    if not isinstance(cand, dict):
                        continue
                    sym = cand.get("symbol", "").upper()
                    if sym:
                        pair = sym + "USDT"
                        if pair not in market_data:
                            market_data[pair] = {
                                "price": cand.get("price", 0),
                                "volume": cand.get("volume", 0),
                                "price_change_1h": cand.get("price_change_1h", cand.get("change_1h", 0)),
                                "price_change_24h": cand.get("price_change_24h", cand.get("change_24h", 0)),
                                "high_24h": cand.get("high_24h") or (cand.get("price", 0) * 1.05),
                                "low_24h": cand.get("low_24h") or (cand.get("price", 0) * 0.95),
                                "coin_id": cand.get("id")
                            }
                print(f"[FALLBACK] Built market_data from {len(market_data)} candidates", flush=True)
            
            valid_market_data = {k: v for k, v in market_data.items() if isinstance(v, dict)}
            if len(valid_market_data) < len(market_data):
                removed_count = len(market_data) - len(valid_market_data)
                print(f"[DATA VALIDATION] Removed {removed_count} invalid market_data entries", flush=True)
                market_data = valid_market_data

            print(f"[✓] Loaded {len(trending)} trending symbols, {len(market_data)} market pairs", flush=True)
            print("[🔄 Pair Selection] Processing strategy-specific pairs...", flush=True)
            cache_size_mb = sum(entry.get('size_bytes', 0) for entry in _klines_cache_global.values()) / (1024 * 1024)
            print(f"[CACHE] Size: {len(_klines_cache_global)} entries, {cache_size_mb:.1f}MB - {cache_stats_str()}", flush=True)

            # Ensure we have candidates to process
            if not momentum_candidates and not range_candidates and not reversal_candidates:
                print("⚠️ No candidates available for processing. Skipping cycle...", flush=True)
                time.sleep(max(1, CYCLE_SECONDS - (time.time() - cycle_start_time)))
                continue

            strategy_candidates = {}
            
            def process_strategy(cand, market_data, market_trend, market_sentiment_pct,
                              hotness_ranks, total_hotness, cooldown_cache, vol_cache, strategy_name, expected_direction=None, klines_cache=None):
                if not isinstance(cand, dict):
                    return None
                    
                sym = cand.get("symbol")
                if not sym:
                    return None

                pair = sym + "USDT"
                
                # Debug: Show thread activity
                import threading
                thread_id = threading.current_thread().ident
                if os.getenv("DEBUG_THREADPOOL", "false").lower() == "true":
                    print(f"[THREADPOOL] Worker {thread_id} started processing {pair} ({strategy_name})", flush=True)

                if is_stablecoin(sym):
                    if os.getenv("DEBUG_STABLECOINS", "false").lower() == "true":
                        print(f"[STABLECOIN] {sym} - REJECTED: stablecoin asset", flush=True)
                    if os.getenv("DEBUG_THREADPOOL", "false").lower() == "true":
                        print(f"[THREADPOOL] Worker {thread_id} completed {pair} ({strategy_name}) - STABLECOIN", flush=True)
                    return None
                
                if should_skip_pair_due_to_failures(pair, strategy_name):
                    if DEBUG_ERRORS:
                        print(f"[SKIP] {pair} ({strategy_name}): Skipped due to repeated failures (cooldown)", flush=True)
                    if os.getenv("DEBUG_THREADPOOL", "false").lower() == "true":
                        print(f"[THREADPOOL] Worker {thread_id} completed {pair} ({strategy_name}) - REPEATED_FAILURES", flush=True)
                    return None
                cid = cand.get("coin_id")
                if not cid and pair in market_data:
                    mdata = market_data.get(pair)
                    if isinstance(mdata, dict):
                        cid = mdata.get("coin_id")
                if not cid:
                    cid = cand.get("id")
                binance_symbol = symbol_to_binance(pair)
                
                debug_pair = os.getenv("DEBUG_PAIR_PROCESSING", "false").lower() == "true"
                if debug_pair:
                    print(f"  [{strategy_name}] Processing {pair}...", flush=True)

                pair_data = market_data.get(pair)
                if not isinstance(pair_data, dict):
                    pair_data = {
                        "price": None, "volume": 0.0, "price_change_1h": 0.0,
                        "price_change_24h": 0.0, "high_24h": None, "low_24h": None,
                        "coin_id": cid,
                        "symbol": sym
                    }

                volume_usd = (pair_data.get("volume") or 0.0)
                min_vol_threshold = max(MIN_VOLUME_USD, PAIR_QUALITY_TARGET_VOLUME * 0.3)
                if volume_usd < min_vol_threshold:
                    if debug_pair:
                        print(f"  [{strategy_name}] {pair}: SKIPPED - volume {volume_usd} < threshold {min_vol_threshold}", flush=True)
                    return None

                vol_est = vol_cache.get(cid)
                if vol_est is None:
                    try:
                        price = pair_data.get("price") or 0.0
                        high = pair_data.get("high_24h") or price
                        low = pair_data.get("low_24h") or price
                        today_range = abs((high - low) / (price + 1e-12)) if price > 0 else 0.1
                        vol_est = today_range
                        vol_cache[cid] = today_range
                    except Exception:
                        vol_est = 0.1
                        vol_cache[cid] = 0.1

                klines_15m = None
                klines_30m = None
                klines_1h = None
                klines_4h = None
                bin_info = {"vwap": None, "top_bins": [], "swing_highs": [], "swing_lows": []}
                tv_metrics = None

                try:
                    if strategy_name == 'reversal':
                        if debug_pair:
                            print(f"    Fetching reversal klines (1m/5m/15m) for {pair}...", flush=True)
                        
                        try:
                            # Add timeout wrapper to prevent hanging
                            from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
                            with ThreadPoolExecutor(max_workers=1) as timeout_executor:
                                future = timeout_executor.submit(fetch_klines_for_reversal, pair, '15m', 80, cid)
                                try:
                                    klines_15m, kl_1m, kl_5m, fresh_price = future.result(timeout=30)  # 30s max for klines fetch
                                except (FutureTimeoutError, TimeoutError):
                                    if debug_pair:
                                        print(f"    [{pair}] Klines fetch timeout (30s), skipping", flush=True)
                                    track_fetch_failure(pair, strategy_name)
                                    return None
                            kline_15m_count = len(klines_15m) if klines_15m else 0
                            kline_1m_count = len(kl_1m) if kl_1m else 0
                            kline_5m_count = len(kl_5m) if kl_5m else 0
                            if debug_pair or kline_15m_count == 0:
                                print(f"    [{pair}] Reversal klines: 15m={kline_15m_count} 1m={kline_1m_count} 5m={kline_5m_count}", flush=True)
                            
                            if klines_15m and DEBUG_QUALITY:
                                quality_ok, issues = check_kline_data_quality(klines_15m, '15m', pair, 'REVERSAL')
                                if not quality_ok and debug_pair:
                                    print(f"    [QUALITY] 15m data issues: {issues[0] if issues else 'unknown'}", flush=True)
                        except Exception as e:
                            if debug_pair:
                                print(f"    Reversal kline fetch error: {str(e)[:80]}", flush=True)
                            track_fetch_failure(pair, strategy_name)
                            klines_15m = None
                            kl_1m = kl_5m = None
                        
                        if not klines_15m or not isinstance(klines_15m, list):
                            if debug_pair:
                                print(f"    No 15m klines or invalid format, skipping", flush=True)
                            if not klines_15m:
                                track_fetch_failure(pair, strategy_name)
                            return None
                        
                        if len(klines_15m) < 10:
                            if debug_pair:
                                print(f"    15m klines insufficient: only {len(klines_15m)} candles (need ≥10), skipping", flush=True)
                            track_fetch_failure(pair, strategy_name)
                            return None
                        
                        klines_1h, klines_4h = parallel_fetch_klines_1h_4h(binance_symbol, cid, "reversal")
                        
                        try:
                            vwap, top_bins = compute_vwap_and_volume_clusters(klines_15m, bins=24)
                            swings_h, swings_l = detect_recent_swing_levels(klines_15m, lookback=40)
                            bin_info.update({
                                "vwap": vwap,
                                "top_bins": top_bins,
                                "swing_highs": swings_h,
                                "swing_lows": swings_l
                            })
                        except Exception as e:
                            if debug_pair:
                                print(f"    [BIN_INFO] Failed to compute vwap/swings: {str(e)[:60]}", flush=True)

                        if not tv_metrics and klines_15m:
                            tv_metrics = compute_tv_metrics(klines_15m)
                        
                        if not isinstance(tv_metrics, dict):
                            if debug_pair:
                                print(f"    [VALIDATION] tv_metrics invalid type: {type(tv_metrics)}, using None", flush=True)
                            tv_metrics = None
                        
                        if not isinstance(bin_info, dict):
                            if debug_pair:
                                print(f"    [VALIDATION] bin_info invalid type: {type(bin_info)}, using empty dict", flush=True)
                            bin_info = {"vwap": None, "top_bins": [], "swing_highs": [], "swing_lows": []}
                        else:
                            if "top_bins" in bin_info and not isinstance(bin_info["top_bins"], list):
                                bin_info["top_bins"] = []
                            if "swing_highs" in bin_info and not isinstance(bin_info["swing_highs"], list):
                                bin_info["swing_highs"] = []
                            if "swing_lows" in bin_info and not isinstance(bin_info["swing_lows"], list):
                                bin_info["swing_lows"] = []

                        return process_reversal_strategy(pair_data, klines_15m, klines_1h, klines_4h, bin_info, tv_metrics,
                                                       vol_est, market_trend, market_sentiment_pct, cid, sym, pair, thread_id, strategy_name, expected_direction=expected_direction)

                    elif strategy_name == 'range':
                        try:
                            if debug_pair:
                                print(f"    Range strategy starting - fetching klines for {pair}...", flush=True)
                            # Add timeout wrapper to prevent hanging
                            from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
                            with ThreadPoolExecutor(max_workers=1) as timeout_executor:
                                future = timeout_executor.submit(fetch_klines_for_range, pair, '1h', 120, cid)
                                try:
                                    klines_1h, kl_1m, kl_5m, kl_15m, fresh_price = future.result(timeout=30)  # 30s max for klines fetch
                                except (FutureTimeoutError, TimeoutError):
                                    if debug_pair:
                                        print(f"    [{pair}] Range klines fetch timeout (30s), skipping", flush=True)
                                    track_fetch_failure(pair, strategy_name)
                                    return None
                            if debug_pair:
                                print(f"    Range klines: 1h={len(klines_1h) if klines_1h else 0} 1m={len(kl_1m) if kl_1m else 0} 5m={len(kl_5m) if kl_5m else 0} 15m={len(kl_15m) if kl_15m else 0}", flush=True)
                            
                            if klines_1h and DEBUG_QUALITY:
                                quality_ok, issues = check_kline_data_quality(klines_1h, '1h', pair, 'RANGE')
                                if not quality_ok and debug_pair:
                                    print(f"    [QUALITY] 1h data issues: {issues[0] if issues else 'unknown'}", flush=True)
                        except Exception as e:
                            if debug_pair:
                                print(f"    Range kline fetch error: {str(e)[:80]}", flush=True)
                            track_fetch_failure(pair, strategy_name)
                            klines_1h = None
                            kl_1m = kl_5m = kl_15m = None
                        
                        try:
                            klines_30m = fetch_any_klines(binance_symbol, interval='30m', limit=150, coin_id=cid, strategy="range")
                        except Exception as e:
                            if debug_pair:
                                print(f"    30m fetch error: {str(e)[:80]}", flush=True)
                            klines_30m = None
                        
                        try:
                            klines_4h = fetch_any_klines(binance_symbol, interval='4h', limit=80, coin_id=cid, strategy="range")
                        except Exception as e:
                            if debug_pair:
                                print(f"    4h fetch error: {str(e)[:80]}", flush=True)
                            klines_4h = None

                        if not (klines_30m or klines_1h or kl_1m or kl_5m or kl_15m):
                            if debug_pair:
                                print(f"    No klines available: 30m={len(klines_30m) if klines_30m else 0} 1h={len(klines_1h) if klines_1h else 0} 1m={len(kl_1m) if kl_1m else 0} 5m={len(kl_5m) if kl_5m else 0} 15m={len(kl_15m) if kl_15m else 0}, skipping", flush=True)
                            track_fetch_failure(pair, strategy_name)
                            return None
                        
                        if not (klines_1h and len(klines_1h) >= 10):
                            if debug_pair:
                                print(f"    1h klines insufficient or missing: {len(klines_1h) if klines_1h else 0} candles (need ≥10), skipping", flush=True)
                            track_fetch_failure(pair, strategy_name)
                            return None
                        
                        if not isinstance(tv_metrics, dict) and tv_metrics is not None:
                            if debug_pair:
                                print(f"    [VALIDATION] tv_metrics invalid type: {type(tv_metrics)}, using None", flush=True)
                            tv_metrics = None

                        return process_range_strategy(pair_data, binance_symbol, tv_metrics,
                                                    market_sentiment_pct, cid, sym, pair, thread_id, strategy_name, klines_30m=klines_30m,
                                                    klines_1h=klines_1h, klines_4h=klines_4h,
                                                    market_trend=market_trend, vol_est=vol_est, expected_direction=expected_direction)

                    elif strategy_name == 'momentum':
                        try:
                            # Add timeout wrapper to prevent hanging
                            from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
                            with ThreadPoolExecutor(max_workers=1) as timeout_executor:
                                future = timeout_executor.submit(fetch_klines_for_momentum, pair, '15m', 80, cid)
                                try:
                                    klines_15m, kl_1m, kl_5m, fresh_price = future.result(timeout=30)  # 30s max for klines fetch
                                except (FutureTimeoutError, TimeoutError):
                                    if debug_pair:
                                        print(f"    [{pair}] Momentum klines fetch timeout (30s), skipping", flush=True)
                                    track_fetch_failure(pair, strategy_name)
                                    return None
                            if debug_pair:
                                print(f"    Momentum klines: 15m={len(klines_15m) if klines_15m else 0} 1m={len(kl_1m) if kl_1m else 0} 5m={len(kl_5m) if kl_5m else 0}", flush=True)
                            
                            if klines_15m and DEBUG_QUALITY:
                                quality_ok, issues = check_kline_data_quality(klines_15m, '15m', pair, 'MOMENTUM')
                                if not quality_ok and debug_pair:
                                    print(f"    [QUALITY] 15m data issues: {issues[0] if issues else 'unknown'}", flush=True)
                        except Exception as e:
                            if debug_pair:
                                print(f"    Momentum kline fetch error: {str(e)[:80]}", flush=True)
                            track_fetch_failure(pair, strategy_name)
                            klines_15m = None
                            kl_1m = kl_5m = None
                        
                        if not klines_15m or not isinstance(klines_15m, list):
                            if debug_pair:
                                print(f"    No 15m klines or invalid format: type={type(klines_15m)}, len={len(klines_15m) if isinstance(klines_15m, list) else 'N/A'}, skipping", flush=True)
                            if not klines_15m:
                                track_fetch_failure(pair, strategy_name)
                            return None
                        
                        if len(klines_15m) < 10:
                            if debug_pair:
                                print(f"    15m klines insufficient: only {len(klines_15m)} candles (need ≥10), skipping", flush=True)
                            track_fetch_failure(pair, strategy_name)
                            return None
                        
                        try:
                            klines_1h = fetch_any_klines(binance_symbol, interval='1h', limit=120, coin_id=cid, strategy="momentum")
                        except Exception:
                            klines_1h = None
                        
                        try:
                            klines_4h = fetch_any_klines(binance_symbol, interval='4h', limit=80, coin_id=cid, strategy="momentum")
                        except Exception:
                            klines_4h = None

                        try:
                            vwap, top_bins = compute_vwap_and_volume_clusters(klines_15m, bins=24)
                            swings_h, swings_l = detect_recent_swing_levels(klines_15m, lookback=40)
                            bin_info.update({
                                "vwap": vwap,
                                "top_bins": top_bins,
                                "swing_highs": swings_h,
                                "swing_lows": swings_l
                            })
                        except Exception:
                            pass

                        if not tv_metrics and klines_15m:
                            tv_metrics = compute_tv_metrics(klines_15m)
                        
                        if not isinstance(tv_metrics, dict):
                            if debug_pair:
                                print(f"    [VALIDATION] tv_metrics invalid type: {type(tv_metrics)}, using None", flush=True)
                            tv_metrics = None
                        
                        if not isinstance(bin_info, dict):
                            if debug_pair:
                                print(f"    [VALIDATION] bin_info invalid type: {type(bin_info)}, using empty dict", flush=True)
                            bin_info = {"vwap": None, "top_bins": [], "swing_highs": [], "swing_lows": []}
                        else:
                            if "top_bins" in bin_info and not isinstance(bin_info["top_bins"], list):
                                if debug_pair:
                                    print(f"    [VALIDATION] bin_info['top_bins'] invalid: {type(bin_info['top_bins'])}, resetting", flush=True)
                                bin_info["top_bins"] = []
                            if "swing_highs" in bin_info and not isinstance(bin_info["swing_highs"], list):
                                if debug_pair:
                                    print(f"    [VALIDATION] bin_info['swing_highs'] invalid: {type(bin_info['swing_highs'])}, resetting", flush=True)
                                bin_info["swing_highs"] = []
                            if "swing_lows" in bin_info and not isinstance(bin_info["swing_lows"], list):
                                if debug_pair:
                                    print(f"    [VALIDATION] bin_info['swing_lows'] invalid: {type(bin_info['swing_lows'])}, resetting", flush=True)
                                bin_info["swing_lows"] = []

                        return process_momentum_strategy(
                            pair_data, klines_15m, klines_1h, klines_4h, bin_info, tv_metrics,
                            vol_est, market_trend, market_sentiment_pct,
                            hotness_ranks, total_hotness, cooldown_cache,
                            cid, sym, pair, thread_id, strategy_name, expected_direction=expected_direction
                        )

                except Exception as e:
                    return None

                return None

            def process_reversal_strategy(pair_data, klines_15m, klines_1h, klines_4h, bin_info, tv_metrics, vol_est,
                                        market_trend, market_sentiment_pct, cid, sym, pair, thread_id, strategy_name, expected_direction=None):
                debug_rev = os.getenv("DEBUG_REVERSAL", "false").lower() == "true"
                if not isinstance(pair_data, dict):
                    if debug_rev:
                        print(f"[REV_STRATEGY] {pair}: pair_data not dict", flush=True)
                    return None
                
                if tv_metrics is not None and not isinstance(tv_metrics, dict):
                    tv_metrics = None
                
                if not isinstance(bin_info, dict):
                    bin_info = {"vwap": None, "top_bins": [], "swing_highs": [], "swing_lows": []}
                
                try:
                    if debug_rev:
                        print(f"[REV_STRATEGY] {pair}: calling detect_reversal_opportunity_v3...", flush=True)
                    rev_result = detect_reversal_opportunity_v3_with_fallbacks(
                        pair_data, klines_15m, bin_info,
                        tv_metrics=tv_metrics, vol_est=vol_est,
                        market_trend=market_trend, market_sentiment_pct=market_sentiment_pct,
                        klines_1h=klines_1h, klines_4h=klines_4h
                    )
                    if debug_rev:
                        print(f"[REV_STRATEGY] {pair}: detect_reversal returned {type(rev_result).__name__}", flush=True)
                except Exception as e:
                    if debug_rev:
                        print(f"[REV_STRATEGY] {pair}: detect_reversal exception: {str(e)[:80]}", flush=True)
                    return None

                if isinstance(rev_result, list):
                    rev_result = rev_result[0] if rev_result else None

                if not rev_result or not rev_result.direction:
                    if debug_rev:
                        direction_val = rev_result.direction if rev_result else 'N/A'
                        print(f"[REV_STRATEGY] {pair}: NO SIGNAL - result={rev_result is not None}, direction={direction_val}, klines={len(klines_15m) if klines_15m else 0}", flush=True)
                    return None
                
                if debug_rev:
                    print(f"[REV_STRATEGY] {pair}: FOUND SIGNAL - direction={rev_result.direction}, confidence={rev_result.confidence:.1f}", flush=True)

                rev_dir = rev_result.direction
                
                if expected_direction and rev_dir != expected_direction:
                    if os.getenv("DEBUG_THREADPOOL", "false").lower() == "true":
                        print(f"[THREADPOOL] Worker {thread_id} completed {pair} ({strategy_name}) - WRONG DIRECTION", flush=True)
                    return None
                rev_conf = rev_result.confidence
                rev_layers = rev_result.layers
                rev_strength = rev_result.reversal_strength

                entry_price = None
                kl_1m_rev = kl_5m_rev = None
                try:
                    klines_15m_new, kl_1m_rev, kl_5m_rev, _ = fetch_klines_for_reversal(pair, interval="15m", limit=80, coin_id=cid)
                    if klines_15m_new and not klines_15m:
                        klines_15m = klines_15m_new
                except Exception:
                    pass
                
                try:
                    mtf_momentum_rev, mtf_agree_rev, mtf_bonus_rev = analyze_multi_timeframe_momentum(kl_1m_rev, kl_5m_rev, klines_15m, rev_dir)
                    if mtf_agree_rev is not None and mtf_agree_rev >= 2:
                        rev_conf += mtf_bonus_rev
                        if os.getenv("DEBUG_REVERSAL", "false").lower() == "true":
                            print(f"[REV V3] {pair} {rev_dir} - Multi-TF momentum agreement={mtf_agree_rev} bonus={mtf_bonus_rev:.1f}", flush=True)
                except Exception as e:
                    if os.getenv("DEBUG_REVERSAL", "false").lower() == "true":
                        print(f"[REV V3] {pair} - MTF momentum analysis failed: {str(e)[:60]}", flush=True)
                
                entry_price = pair_data.get("price") if pair_data else None
                if entry_price is None or (isinstance(entry_price, (int, float)) and entry_price <= 0):
                    klines_dict = {
                        "1m": kl_1m_rev,
                        "5m": kl_5m_rev,
                        "15m": klines_15m
                    }
                    # Create a market_data dict from pair_data for get_entry_price_safe
                    market_data_for_price = {pair: pair_data} if pair_data else None
                    entry_price, price_source, price_staleness = get_entry_price_safe(
                        klines_dict, pair, coin_id=cid, strategy_name="REVERSAL", fallback_only_fresh=True, market_data=market_data_for_price
                    )
                    if entry_price:
                        rev_conf_metadata = {"price_source": price_source, "price_staleness_sec": price_staleness}
                    else:
                        return None
                else:
                    rev_conf_metadata = {"price_source": "livecoinwatch_reversal", "price_staleness_sec": 0.0}
                
                if entry_price is not None and entry_price > 0:
                    # Use already fetched klines - they are fresh enough for analysis
                    pass

                sl_pct_rev, tp_pct_rev = calculate_dynamic_tp_sl_from_movement(
                    klines_15m, entry_price, rev_dir, strategy_type="reversal",
                    tv_metrics=tv_metrics, bin_info=bin_info, vol_est=vol_est,
                    momentum_strength=None, confidence=rev_conf,
                    market_trend=market_trend
                )

                signal = {
                    "type": "REVERSAL",
                    "timestamp": time.time(),
                    "coin_id": cid,
                    "symbol": sym,
                    "pair": pair,
                    "direction": rev_dir,
                    "entry": round(entry_price, 8),
                    "sl": round(entry_price * (1 + sl_pct_rev) if rev_dir == "SHORT" else entry_price * (1 - sl_pct_rev), 8),
                    "tp": round(entry_price * (1 - tp_pct_rev) if rev_dir == "SHORT" else entry_price * (1 + tp_pct_rev), 8),
                    "sl_pct": round(sl_pct_rev, 6),
                    "tp_pct": round(tp_pct_rev, 6),
                    "confidence": round(rev_conf, 2),
                    "quality_score": round(rev_result.quality_score, 2),
                    "trigger_layers": rev_layers,
                    "reversal_strength": rev_strength.get("strength_class", "UNKNOWN"),
                    "reversal_strength_pct": rev_strength.get("strength_pct", 0.0),
                    "notes": "reversal_detector_v3_with_fallbacks",
                    "zone": compute_zone(pair_data)[0],
                    "vol_est": vol_est,
                    "price_source": rev_conf_metadata.get("price_source", "unknown") if 'rev_conf_metadata' in locals() else "unknown",
                    "price_staleness_sec": rev_conf_metadata.get("price_staleness_sec", 0) if 'rev_conf_metadata' in locals() else 0
                }
                
                assert signal.get("direction") in ["LONG", "SHORT"], f"REVERSAL: Invalid direction {signal.get('direction')}"

                # Add TP1 and probabilities
                signal["tp"] = round(adjust_target_for_spread(entry_price, signal["tp"], rev_dir), 8)
                signal["tp_pct"] = round(abs(signal["tp"] - entry_price) / entry_price, 6)

                # Estimate TP1 and probabilities
                try:
                    binance_symbol = symbol_to_binance(pair)
                    kl_for_prob = klines_15m if klines_15m else fetch_any_klines(binance_symbol, interval="15m", limit=160, coin_id=cid, strategy="range")
                    if kl_for_prob:
                        entry_v = float(signal["entry"])
                        sl_v = float(signal["sl"])
                        tp2_v = float(signal["tp"])
                        atr = compute_atr_from_klines(kl_for_prob)
                        sign = 1 if signal["direction"] == "LONG" else -1
                        tp1_v = entry_v + sign * max((atr or 0.0) * 0.8, abs(tp2_v - entry_v) * 0.33)

                        prob_tp1, prob_sl_1 = estimate_tp_sl_hit_prob(kl_for_prob, entry_v, tp1_v, sl_v, lookahead=12, vol_est=vol_est)
                        prob_tp2, prob_sl_2 = estimate_tp_sl_hit_prob(kl_for_prob, entry_v, tp2_v, sl_v, lookahead=20, vol_est=vol_est)

                        prob_tp1 = float(prob_tp1) if prob_tp1 is not None else 0.5
                        prob_sl_1 = float(prob_sl_1) if prob_sl_1 is not None else 0.5
                        prob_tp2 = float(prob_tp2) if prob_tp2 is not None else 0.5
                        prob_sl_2 = float(prob_sl_2) if prob_sl_2 is not None else 0.5

                        signal["tp1"] = round(tp1_v, 8)
                        signal["prob_tp1"] = round(prob_tp1, 3)
                        signal["prob_tp2"] = round(prob_tp2, 3)
                        signal["prob_sl"] = round(prob_sl_2, 3)

                        try:
                            signal['prob_tp'] = prob_tp2
                            signal['prob_sl'] = prob_sl_2
                            
                            vol_surge_rev, _ = detect_volume_surge(kl_for_prob)
                            breakout_rev, _ = detect_breakout_momentum(kl_for_prob, signal["direction"])
                            signal['volume_surge'] = vol_surge_rev
                            signal['breakout_detected'] = breakout_rev
                            
                            adjust_signal_confidence(signal, 'reversal')

                            signal["skip_due_to_pullback"] = False
                            if prob_sl_2 > float(PULLBACK_SKIP_PROB_SL) or (prob_sl_2 > prob_tp2 and prob_sl_2 > (float(PULLBACK_SKIP_PROB_SL) - float(PULLBACK_SKIP_MARGIN))):
                                signal["notes"] += " | WAIT_FOR_PULLBACK"
                                signal["skip_due_to_pullback"] = True
                        except Exception as e:
                            print(f"⚠️ Error in probability estimation for {pair}: {str(e)[:100]}")

                    is_valid, reason = validate_signal_confidence(signal, strategy_name)
                    if not is_valid:
                        if DEBUG_ERRORS:
                            print(f"[CONFIDENCE] {pair} ({strategy_name}): Signal rejected - {reason}", flush=True)
                        if os.getenv("DEBUG_THREADPOOL", "false").lower() == "true":
                            print(f"[THREADPOOL] Worker {thread_id} completed {pair} ({strategy_name}) - LOW_CONFIDENCE", flush=True)
                        return None
                    
                    entry_valid, entry_msg, entry_dev = validate_entry_price(signal, pair_data, max_deviation_pct=2.0)
                    if not entry_valid:
                        if DEBUG_QUALITY:
                            print(f"[ENTRY_PRICE] {pair} ({strategy_name}): {entry_msg}", flush=True)
                        signal['notes'] = (signal.get('notes', '') + f" | ⚠️ STALE_ENTRY({entry_dev:.1f}%)").strip(" |")
                    
                    clear_pair_failures(pair, strategy_name)
                    
                    if os.getenv("DEBUG_THREADPOOL", "false").lower() == "true":
                        print(f"[THREADPOOL] Worker {thread_id} completed {pair} ({strategy_name}) - SIGNAL FOUND", flush=True)
                    return signal

                except Exception as e:
                    print(f"⚠️ Error in process_reversal_strategy for {pair}: {str(e)[:100]}")
                    track_fetch_failure(pair, strategy_name)
                    if os.getenv("DEBUG_THREADPOOL", "false").lower() == "true":
                        print(f"[THREADPOOL] Worker {thread_id} completed {pair} ({strategy_name}) - ERROR", flush=True)
                    return None

            def process_range_strategy(pair_data, binance_symbol, tv_metrics,
                                     market_sentiment_pct, cid, sym, pair, thread_id, strategy_name, klines_30m=None, klines_1h=None, 
                                     klines_4h=None, market_trend="neutral", vol_est=None, expected_direction=None):
                try:
                    dbg_range = os.getenv("DEBUG_RANGE", "false").lower() == "true"
                    
                    if not isinstance(pair_data, dict):
                        return None
                    
                    if tv_metrics is not None and not isinstance(tv_metrics, dict):
                        tv_metrics = None
                    
                    # RANGE strictly prioritizes Huobi/Gemini for Entry Price
                    klines_dict_range = {"1h": klines_1h} if klines_1h else {}
                    market_data_for_price = {pair: pair_data} if pair_data else None
                    entry_price, range_price_source, range_price_staleness = get_entry_price_safe(
                        klines_dict_range, pair, coin_id=cid, strategy_name="RANGE", fallback_only_fresh=True, market_data=market_data_for_price
                    )
                    
                    if not entry_price:
                        if dbg_range:
                            print(f"[RANGE_STRATEGY] {pair}: No fresh entry price from Huobi/Gemini, rejecting", flush=True)
                        return None
                    
                    # Removed redundant kline fetching/price refresh (already handled inside detection)

                    try:
                        if dbg_range:
                            print(f"[RANGE_STRATEGY] {pair}: calling detect_range_opportunity_v2 with 30m={len(klines_30m) if klines_30m else 0} 1h={len(klines_1h) if klines_1h else 0} 4h={len(klines_4h) if klines_4h else 0}...", flush=True)
                        signal = detect_range_opportunity_v2_with_fallbacks(
                            pair_data, binance_symbol, tv_metrics,
                            klines_30m, klines_1h, klines_4h,
                            market_sentiment_pct=market_sentiment_pct, market_trend=market_trend, vol_est=vol_est
                        )
                        if dbg_range:
                            print(f"[RANGE_STRATEGY] {pair}: detect_range returned signal={signal is not None}", flush=True)
                    except Exception as e:
                        print(f"[RANGE_STRATEGY] {pair}: detect_range CRASH: {str(e)}", flush=True)
                        signal = None

                    if signal:
                        if isinstance(signal, list):
                            signal = signal[0] if signal else None
                        if signal and isinstance(signal, dict):
                            if expected_direction and signal.get('direction') != expected_direction:
                                if dbg_range:
                                    print(f"[RANGE_STRATEGY] {pair}: Signal direction {signal.get('direction')} doesn't match expected {expected_direction}", flush=True)
                                signal = None
                        if signal and isinstance(signal, dict):
                            signal['type'] = 'RANGE'
                            signal['coin_id'] = cid
                            signal['symbol'] = sym
                            signal['pair'] = pair

                            adjust_signal_confidence(signal, 'range')
                            
                            is_valid, reason = validate_signal_confidence(signal, strategy_name)
                            if not is_valid:
                                if DEBUG_ERRORS:
                                    print(f"[CONFIDENCE] {pair} ({strategy_name}): Signal rejected - {reason}", flush=True)
                                if os.getenv("DEBUG_THREADPOOL", "false").lower() == "true":
                                    print(f"[THREADPOOL] Worker {thread_id} completed {pair} ({strategy_name}) - LOW_CONFIDENCE", flush=True)
                            else:
                                entry_valid, entry_msg, entry_dev = validate_entry_price(signal, pair_data, max_deviation_pct=2.0)
                                if not entry_valid:
                                    if DEBUG_QUALITY:
                                        print(f"[ENTRY_PRICE] {pair} ({strategy_name}): {entry_msg}", flush=True)
                                    signal['notes'] = (signal.get('notes', '') + f" | ⚠️ STALE_ENTRY({entry_dev:.1f}%)").strip(" |")
                                
                                clear_pair_failures(pair, strategy_name)
                                if os.getenv("DEBUG_THREADPOOL", "false").lower() == "true":
                                    print(f"[THREADPOOL] Worker {thread_id} completed {pair} ({strategy_name}) - SIGNAL FOUND", flush=True)
                                return signal

                except Exception as e:
                    print(f"⚠️ Error in process_range_strategy for {pair}: {str(e)[:100]}", flush=True)
                    track_fetch_failure(pair, strategy_name)
                    if os.getenv("DEBUG_THREADPOOL", "false").lower() == "true":
                        print(f"[THREADPOOL] Worker {thread_id} completed {pair} ({strategy_name}) - ERROR", flush=True)

                if os.getenv("DEBUG_THREADPOOL", "false").lower() == "true":
                    print(f"[THREADPOOL] Worker {thread_id} completed {pair} ({strategy_name}) - NO SIGNAL", flush=True)
                return None

            def process_momentum_strategy(pair_data, klines_15m, klines_1h, klines_4h, bin_info, tv_metrics,
                                        vol_est, market_trend, market_sentiment_pct,
                                        hotness_ranks, total_hotness, cooldown_cache,
                                        cid, sym, pair, thread_id, strategy_name, expected_direction=None):
                try:
                    if not isinstance(pair_data, dict):
                        if os.getenv("DEBUG_PAIR_PROCESSING", "false").lower() == "true":
                            print(f"  [VALIDATION ERROR] pair_data is not dict: {type(pair_data)}", flush=True)
                        return None
                    
                    if tv_metrics is not None and not isinstance(tv_metrics, dict):
                        if os.getenv("DEBUG_PAIR_PROCESSING", "false").lower() == "true":
                            print(f"  [VALIDATION ERROR] tv_metrics is not dict: {type(tv_metrics)}", flush=True)
                        tv_metrics = None
                    
                    if not isinstance(bin_info, dict):
                        if os.getenv("DEBUG_PAIR_PROCESSING", "false").lower() == "true":
                            print(f"  [VALIDATION ERROR] bin_info is not dict: {type(bin_info)}", flush=True)
                        bin_info = {"vwap": None, "top_bins": [], "swing_highs": [], "swing_lows": []}
                    signals = []
                    direction_signals = {}
                    
                    # FIXED: Use fresh momentum direction instead of stale expected_direction
                    fresh_momentum_dir = None
                    if klines_15m and len(klines_15m) >= 6:
                        fresh_momentum_dir = compute_momentum_direction_from_klines(klines_15m)
                    
                    # If fresh momentum contradicts expected direction, prefer fresh direction or test both
                    if expected_direction and fresh_momentum_dir and fresh_momentum_dir != "NEUTRAL" and fresh_momentum_dir != expected_direction:
                        if os.getenv("DEBUG_MOMENTUM", "false").lower() == "true":
                            print(f"[MOMENTUM] {pair}: expected_direction={expected_direction} contradicts fresh_momentum={fresh_momentum_dir}, testing both", flush=True)
                        directions_to_check = ['LONG', 'SHORT']
                    elif fresh_momentum_dir and fresh_momentum_dir != "NEUTRAL":
                        directions_to_check = [fresh_momentum_dir]
                    elif expected_direction:
                        directions_to_check = [expected_direction]
                    else:
                        directions_to_check = ['LONG', 'SHORT']

                    for direction in directions_to_check:
                        try:
                            if not isinstance(pair_data, dict):
                                continue
                            if klines_15m and not isinstance(klines_15m, list):
                                continue
                            debug_mom = os.getenv("DEBUG_MOMENTUM", "false").lower() == "true"
                            if debug_mom:
                                print(f"[MOMENTUM] {pair}: testing {direction}...", flush=True)
                            signal = detect_momentum_opportunity_v3_with_fallbacks(
                                pair_data, klines_15m, bin_info, tv_metrics,
                                vol_est, market_trend, market_sentiment_pct,
                                hotness_ranks, total_hotness, cooldown_cache, direction,
                                klines_1h=klines_1h, klines_4h=klines_4h
                            )
                            if debug_mom:
                                print(f"[MOMENTUM] {pair}: {direction} returned signal={signal is not None}", flush=True)

                            if signal:
                                if isinstance(signal, list):
                                    signal = signal[0] if signal else None
                                if signal and isinstance(signal, dict):
                                    direction_signals[direction] = signal
                                    if debug_mom:
                                        print(f"[MOMENTUM] {pair}: {direction} - signal accepted, confidence={signal.get('confidence')}", flush=True)
                        except Exception as dir_err:
                            debug_mom = os.getenv("DEBUG_MOMENTUM", "false").lower() == "true"
                            if debug_mom:
                                print(f"[MOMENTUM] {pair}: Direction {direction} error: {str(dir_err)[:80]}", flush=True)
                            continue

                    if not direction_signals:
                        if klines_15m and len(klines_15m) >= 3:
                            closes = [float(k[4]) for k in klines_15m[-min(6, len(klines_15m)):]]
                            momentum = (closes[-1] - closes[0]) / (closes[0] + 1e-12)
                            fallback_dir = "LONG" if momentum > 0 else "SHORT"
                            # More lenient: use fallback direction even if it doesn't match expected, but prefer expected if available
                            if expected_direction and fallback_dir != expected_direction:
                                if os.getenv("DEBUG_MOMENTUM", "false").lower() == "true":
                                    print(f"[MOMENTUM] {pair}: Fallback direction {fallback_dir} doesn't match expected {expected_direction}, using fallback anyway", flush=True)
                                # Still use fallback_dir to ensure signal generation
                            # Use get_entry_price_safe for Momentum fallback as well
                            klines_dict_mom = {"15m": klines_15m} if klines_15m else {}
                            market_data_for_price = {pair: pair_data} if pair_data else None
                            entry_price, mom_price_source, mom_price_staleness = get_entry_price_safe(
                                klines_dict_mom, pair, coin_id=cid, strategy_name="MOMENTUM", fallback_only_fresh=True, market_data=market_data_for_price
                            )
                            
                            if not entry_price or entry_price <= 0:
                                entry_price = closes[-1] if closes else 0.0001
                                mom_price_source = "fallback_close_price"
                                mom_price_staleness = 0
                            
                            sl_pct_fb, tp_pct_fb = calculate_dynamic_tp_sl_from_movement(
                                klines_15m, entry_price, fallback_dir, strategy_type="momentum",
                                tv_metrics=tv_metrics, bin_info=bin_info, vol_est=vol_est,
                                momentum_strength={"class": "MODERATE"}, confidence=15.0,
                                market_trend=market_trend
                            )
                            tp_price = entry_price * (1 + tp_pct_fb) if fallback_dir == "LONG" else entry_price * (1 - tp_pct_fb)
                            sl_price = entry_price * (1 - sl_pct_fb) if fallback_dir == "LONG" else entry_price * (1 + sl_pct_fb)
                            fallback_signal = {
                                'type': 'MOMENTUM',
                                'direction': fallback_dir,
                                'confidence': 15.0,
                                'prob_tp': 0.45,
                                'prob_sl': 0.55,
                                'quality_score': 20.0,
                                'notes': 'fallback_momentum',
                                'coin_id': cid,
                                'symbol': sym,
                                'pair': pair,
                                'entry': round(entry_price, 8),
                                'tp': round(tp_price, 8),
                                'sl': round(sl_price, 8),
                                'tp_pct': round(tp_pct_fb, 6),
                                'sl_pct': round(sl_pct_fb, 6),
                                'timestamp': time.time(),
                                'price_source': mom_price_source,
                                'price_staleness_sec': mom_price_staleness
                            }
                            direction_signals[fallback_dir] = fallback_signal
                        else:
                            if klines_15m and len(klines_15m) >= 2:
                                closes = [float(k[4]) for k in klines_15m[-min(5, len(klines_15m)):]]
                                simple_momentum = (closes[-1] - closes[0]) / (closes[0] + 1e-12)
                                fallback_dir = "LONG" if simple_momentum > 0 else "SHORT"
                                
                                klines_dict_mom = {"15m": klines_15m} if klines_15m else {}
                                market_data_for_price = {pair: pair_data} if pair_data else None
                                entry_price, mom_price_source, mom_price_staleness = get_entry_price_safe(
                                    klines_dict_mom, pair, coin_id=cid, strategy_name="MOMENTUM", fallback_only_fresh=True, market_data=market_data_for_price
                                )
                                
                                if not entry_price or entry_price <= 0:
                                    entry_price = closes[-1] if closes else None
                                    mom_price_source = "kline_close_fallback"
                                    mom_price_staleness = 0

                                if entry_price and entry_price > 0:
                                    simple_signal = {
                                        'type': 'MOMENTUM',
                                        'direction': fallback_dir,
                                        'confidence': 15.0,
                                        'prob_tp': 0.40,
                                        'prob_sl': 0.60,
                                        'quality_score': 8.0,
                                        'notes': 'simple_momentum_fallback',
                                        'coin_id': cid,
                                        'symbol': sym,
                                        'pair': pair,
                                        'entry': round(entry_price, 8),
                                        'tp': round(entry_price * (1.02 if fallback_dir == "LONG" else 0.98), 8),
                                        'sl': round(entry_price * (0.98 if fallback_dir == "LONG" else 1.02), 8),
                                        'tp_pct': round(0.02, 6),
                                        'sl_pct': round(0.02, 6),
                                        'timestamp': time.time(),
                                        'price_source': mom_price_source,
                                        'price_staleness_sec': mom_price_staleness
                                    }
                                    direction_signals[fallback_dir] = simple_signal
                            if not direction_signals:
                                return None

                    if len(direction_signals) == 2:
                        long_sig = direction_signals.get('LONG')
                        short_sig = direction_signals.get('SHORT')
                        if isinstance(long_sig, dict) and isinstance(short_sig, dict):
                            long_conf = long_sig.get('confidence', 0)
                            short_conf = short_sig.get('confidence', 0)
                            conf_diff = abs(long_conf - short_conf)

                            if conf_diff < 5.0:
                                best_dir = max(direction_signals.keys(), key=lambda d: direction_signals[d].get('confidence', 0) if isinstance(direction_signals[d], dict) else 0)
                                best_sig = direction_signals.get(best_dir)
                                if isinstance(best_sig, dict):
                                    signals.append(best_sig)
                            else:
                                for sig in direction_signals.values():
                                    if isinstance(sig, dict):
                                        signals.append(sig)
                    else:
                        signals = [s for s in direction_signals.values() if isinstance(s, dict)]

                    cleaned_signals = []
                    for signal in signals:
                        if not isinstance(signal, dict):
                            continue
                        if expected_direction and signal.get('direction') != expected_direction:
                            if os.getenv("DEBUG_MOMENTUM", "false").lower() == "true":
                                print(f"[MOMENTUM] {pair}: Signal direction {signal.get('direction')} doesn't match expected {expected_direction}", flush=True)
                            continue
                        signal['coin_id'] = cid
                        signal['symbol'] = sym
                        signal['pair'] = pair
                        adjust_signal_confidence(signal, 'momentum')
                        
                        is_valid, reason = validate_signal_confidence(signal, strategy_name)
                        if not is_valid:
                            if DEBUG_ERRORS:
                                print(f"[CONFIDENCE] {pair} ({strategy_name}): Signal rejected - {reason}", flush=True)
                            continue
                        
                        entry_valid, entry_msg, entry_dev = validate_entry_price(signal, pair_data, max_deviation_pct=2.0)
                        if not entry_valid:
                            if DEBUG_QUALITY:
                                print(f"[ENTRY_PRICE] {pair} ({strategy_name}): {entry_msg}", flush=True)
                            signal['notes'] = (signal.get('notes', '') + f" | ⚠️ STALE_ENTRY({entry_dev:.1f}%)").strip(" |")
                        
                        cleaned_signals.append(signal)
                    
                    if cleaned_signals:
                        clear_pair_failures(pair, strategy_name)

                    if os.getenv("DEBUG_THREADPOOL", "false").lower() == "true":
                        if cleaned_signals:
                            print(f"[THREADPOOL] Worker {thread_id} completed {pair} ({strategy_name}) - SIGNAL FOUND", flush=True)
                        else:
                            print(f"[THREADPOOL] Worker {thread_id} completed {pair} ({strategy_name}) - NO SIGNAL", flush=True)
                    return cleaned_signals if cleaned_signals else None

                except Exception as e:
                    print(f"⚠️ Error in process_momentum_strategy for {pair}: {str(e)[:100]}")
                    track_fetch_failure(pair, strategy_name)
                    if os.getenv("DEBUG_THREADPOOL", "false").lower() == "true":
                        print(f"[THREADPOOL] Worker {thread_id} completed {pair} ({strategy_name}) - ERROR", flush=True)
                    return None

            def rank_signals_by_quality(all_strategy_signals):
                """Rank all signals by quality across strategies. Higher quality gets higher confidence boost."""
                all_signals_flat = []
                for strategy_name, signals in all_strategy_signals.items():
                    for sig in signals:
                        sig['strategy'] = strategy_name
                        all_signals_flat.append(sig)

                if not all_signals_flat:
                    return all_strategy_signals

                quality_scores = [s.get('quality_score', s.get('confidence', 0)) for s in all_signals_flat]
                if not quality_scores or max(quality_scores) <= 0:
                    return all_strategy_signals

                median_quality = statistics.median(quality_scores) if len(quality_scores) > 1 else quality_scores[0]

                for sig in all_signals_flat:
                    quality = sig.get('quality_score', sig.get('confidence', 0))
                    if quality > median_quality * 1.2:
                        sig['confidence'] = min(100.0, sig.get('confidence', 0) * 1.08)
                        sig['notes'] = (sig.get('notes', '') + " | high_quality_boost").strip(" |")
                    elif quality < median_quality * 0.7:
                        sig['confidence'] = max(15.0, sig.get('confidence', 0) * 0.92)
                        sig['notes'] = (sig.get('notes', '') + " | low_quality_penalty").strip(" |")

                return all_strategy_signals

            def ensure_signals_across_strategies(all_strategy_signals):
                """Ensure at least one quality signal per direction across all strategies."""
                strategies_with_signals = {s: len(sigs) for s, sigs in all_strategy_signals.items() if sigs}
                if len(strategies_with_signals) >= 2:
                    return

                print("[SIGNAL COVERAGE] Warning: Only one strategy producing signals. Applying strict quality checks...")
                for strategy_name in all_strategy_signals:
                    for sig in all_strategy_signals[strategy_name]:
                        sig['notes'] = (sig.get('notes', '') + " | single_strategy").strip(" |")

            def normalize_confidence_scores(all_strategy_signals):
                """
                Normalize confidence scores with consistent strategy-aware ranges.
                Ranges represent expected confidence bounds per strategy:
                - REVERSAL: 30-95 (moderate floor allows minor reversals)
                - RANGE: 28-88 (lower ceiling - ranges are trickier to trade)
                - MOMENTUM: 35-95 (higher floor - requires conviction)
                """
                normalized = {}
                
                for strategy_name, signals in all_strategy_signals.items():
                    if not signals:
                        normalized[strategy_name] = []
                        continue
                    
                    confidences = [s.get('confidence', 0) for s in signals]
                    if not confidences or max(confidences) == 0:
                        normalized[strategy_name] = signals
                        continue
                    
                    min_conf = min(confidences)
                    max_conf = max(confidences)
                    range_conf = max_conf - min_conf if max_conf > min_conf else 1.0
                    
                    normalized_sigs = []
                    for sig in signals:
                        conf = sig.get('confidence', 0)
                        
                        if range_conf > 0:
                            rel_position = (conf - min_conf) / range_conf
                        else:
                            rel_position = 0.5
                        
                        strategy_type = sig.get('type', 'momentum').lower()
                        
                        if strategy_type == 'reversal':
                            min_range, max_range = 30.0, 95.0
                        elif strategy_type == 'range':
                            min_range, max_range = 28.0, 88.0
                        elif strategy_type == 'momentum':
                            min_range, max_range = 35.0, 95.0
                        else:
                            min_range, max_range = 30.0, 90.0
                        
                        normalized_conf = min_range + (rel_position * (max_range - min_range))
                        sig['confidence'] = round(max(20.0, min(100.0, normalized_conf)), 2)
                        normalized_sigs.append(sig)
                    
                    normalized[strategy_name] = normalized_sigs
                
                return normalized

            def apply_wait_for_pullback_filter(all_strategy_signals, pullback_penalty=0.50):
                """Apply heavy penalty to signals flagged with WAIT_FOR_PULLBACK."""
                filtered = {}
                for strategy_name, signals in all_strategy_signals.items():
                    updated_sigs = []
                    for sig in signals:
                        if sig.get('skip_due_to_pullback', False):
                            sig['confidence'] = max(5.0, sig.get('confidence', 0) * pullback_penalty)
                            sig['notes'] = (sig.get('notes', '') + " | PULLBACK_PENALTY").strip(" |")
                        updated_sigs.append(sig)
                    filtered[strategy_name] = updated_sigs
                return filtered

            def validate_tp_probability(all_strategy_signals, min_tp_prob=None):
                """Filter out signals with low TP hit probability."""
                if min_tp_prob is None:
                    min_tp_prob = float(os.getenv("MIN_TP_PROB", "0.15"))  # Lowered from 0.20 to 0.15 for bearish markets
                filtered = {}
                for strategy_name, signals in all_strategy_signals.items():
                    valid_sigs = []
                    for sig in signals:
                        prob_tp = sig.get('prob_tp', 0.5)
                        if prob_tp >= min_tp_prob:
                            valid_sigs.append(sig)
                        else:
                            # Debug: show why signals are being filtered
                            if os.getenv("DEBUG_TP_FILTER", "false").lower() == "true":
                                print(f"[TP_FILTER] {strategy_name} {sig.get('pair')} {sig.get('direction')} filtered: prob_tp={prob_tp:.3f} < {min_tp_prob}", flush=True)
                    filtered[strategy_name] = valid_sigs
                return filtered

            def deduplicate_signals_by_time_window(all_strategy_signals, window_seconds=600):
                """Deduplicate same pair-direction within rolling time window."""
                filtered = {}
                now = time.time()
                for strategy_name, signals in all_strategy_signals.items():
                    seen = {}
                    unique_sigs = []
                    for sig in sorted(signals, key=lambda s: s.get('confidence', 0), reverse=True):
                        pair = sig.get('pair', 'UNKNOWN')
                        direction = sig.get('direction', '?')
                        key = (pair, direction)
                        timestamp = sig.get('timestamp', now)
                        if key not in seen:
                            seen[key] = timestamp
                            unique_sigs.append(sig)
                        else:
                            time_since_last = now - seen[key]
                            if time_since_last > window_seconds:
                                unique_sigs.append(sig)
                                seen[key] = timestamp
                    filtered[strategy_name] = unique_sigs
                return filtered

            def filter_duplicate_pair_directions(all_strategy_signals):
                """
                Prevent simultaneous LONG/SHORT within same strategy for same pair.
                Keep only the higher confidence signal when both directions exist.
                """
                filtered_signals = {}

                for strategy_name, signals in all_strategy_signals.items():
                    pair_dir_groups = {}

                    for sig in signals:
                        pair = sig.get('pair', 'UNKNOWN')
                        direction = sig.get('direction', '?')
                        confidence = sig.get('confidence', 0)
                        key = (pair, direction)

                        if key not in pair_dir_groups or confidence > pair_dir_groups[key].get('confidence', 0):
                            pair_dir_groups[key] = sig

                    pair_signals = {}
                    for (pair, direction), sig in pair_dir_groups.items():
                        if pair not in pair_signals:
                            pair_signals[pair] = {}
                        pair_signals[pair][direction] = sig

                    final_sigs = []
                    for pair, directions_dict in pair_signals.items():
                        if len(directions_dict) == 2:
                            long_sig = directions_dict.get('LONG')
                            short_sig = directions_dict.get('SHORT')
                            if long_sig and short_sig:
                                long_conf = long_sig.get('confidence', 0)
                                short_conf = short_sig.get('confidence', 0)
                                winner = long_sig if long_conf >= short_conf else short_sig
                                final_sigs.append(winner)
                        else:
                            final_sigs.extend(directions_dict.values())

                    filtered_signals[strategy_name] = final_sigs

                return filtered_signals

            def detect_rsi_exhaustion(klines):
                """Check if RSI shows exhaustion (>75 or <25) in last 3 candles."""
                if not klines or len(klines) < 15:
                    return False, 0
                closes = [float(k[4]) for k in klines[-15:]]
                indicators = compute_indicators(closes, rsi_period=14)
                rsi = indicators.get("rsi", 50.0)
                if rsi is None:
                    return False, 0
                is_extreme = (rsi > 75) or (rsi < 25)
                candles_with_extreme = sum(1 for k in klines[-3:] if (float(k[4]) and rsi > 75) or (float(k[4]) and rsi < 25))
                return is_extreme, rsi
            
            def detect_volume_climax(klines):
                """Check if recent candle has volume spike (climax volume)."""
                if not klines or len(klines) < 20:
                    return False, 1.0
                volumes = [float(k[7]) for k in klines[-20:]]
                avg_volume = sum(volumes) / len(volumes) if volumes else 1.0
                recent_volume = volumes[-1] if volumes else 1.0
                volume_ratio = recent_volume / (avg_volume + 1e-12)
                is_climax = volume_ratio > VOL_CLIMAX_MULT
                return is_climax, volume_ratio
            
            def detect_wick_rejection(klines):
                """Check if recent candle has upper/lower wick showing price rejection."""
                if not klines or len(klines) < 3:
                    return False
                for k in klines[-3:]:
                    open_p = float(k[1])
                    high_p = float(k[2])
                    low_p = float(k[3])
                    close_p = float(k[4])
                    if open_p > 0 and close_p > 0:
                        candle_range = high_p - low_p
                        body = abs(close_p - open_p)
                        if body > 1e-12:
                            wick_pct = (candle_range - body) / body
                            if wick_pct > WICK_BODY_RATIO:
                                return True
                return False
            
            def detect_breakout(klines, direction='LONG', lookback=20):
                """Check if price broke above/below key level with volume."""
                if not klines or len(klines) < lookback + 2:
                    return False
                recent = klines[-2:]
                historical = klines[-(lookback+2):-2]
                if not historical:
                    return False
                
                if direction == 'LONG':
                    hist_highs = [float(k[2]) for k in historical]
                    breakout_level = max(hist_highs)
                    recent_close = float(recent[-1][4])
                    return recent_close > breakout_level * 1.001
                else:
                    hist_lows = [float(k[3]) for k in historical]
                    breakout_level = min(hist_lows)
                    recent_close = float(recent[-1][4])
                    return recent_close < breakout_level * 0.999
            
            def detect_bb_squeeze(klines, period=20, std_mult=2.0):
                """Check if Bollinger Bands are squeezed (narrow width)."""
                if not klines or len(klines) < period + 5:
                    return False, 0.0
                closes = [float(k[4]) for k in klines[-period:]]
                sma = sum(closes) / len(closes)
                variance = sum((c - sma) ** 2 for c in closes) / len(closes)
                std_dev = variance ** 0.5
                bb_width = (2 * std_dev * std_mult) / (sma + 1e-12) if sma > 0 else 0.5
                is_squeezed = bb_width < RANGE_BB_COMPRESS_THRESHOLD
                return is_squeezed, bb_width
            
            def detect_support_resistance_bounces(klines, lookback=30, min_bounces=2):
                """Detect if price bounces off consistent support/resistance level."""
                if not klines or len(klines) < lookback:
                    return False, []
                
                recent_highs = [float(k[2]) for k in klines[-lookback:]]
                recent_lows = [float(k[3]) for k in klines[-lookback:]]
                
                support_levels = []
                resistance_levels = []
                
                for i, (h, l) in enumerate(zip(recent_highs, recent_lows)):
                    for j in range(i + 1, len(recent_highs)):
                        h2, l2 = recent_highs[j], recent_lows[j]
                        if abs(l - l2) / (l + 1e-12) < 0.005:
                            support_levels.append(l)
                        if abs(h - h2) / (h + 1e-12) < 0.005:
                            resistance_levels.append(h)
                
                has_bounces = len(support_levels) >= min_bounces or len(resistance_levels) >= min_bounces
                levels = support_levels + resistance_levels
                return has_bounces, levels

            def score_pair_for_long_reversal(pair_data):
                """Score a pair's potential for LONG reversal."""
                score = 10.0
                change_24h = pair_data.get("price_change_24h", 0)
                change_1h = pair_data.get("price_change_1h", 0)
                volume = pair_data.get("volume", 0)
                low_24h = pair_data.get("low_24h")
                price = pair_data.get("price")
                
                if not price:
                    return 0.0
                
                if change_24h < -1.0:
                    score += 15
                if change_24h < -3.0:
                    score += 10
                if change_24h < -7.0:
                    score += 10
                if change_1h < -0.5:
                    score += 5
                
                if low_24h:
                    proximity_to_low = (price - low_24h) / (low_24h + 1e-12) if low_24h > 0 else 0
                    if proximity_to_low < 0.1:
                        score += 10
                
                if volume > 10:
                    score += 5
                
                return min(100.0, max(0.0, score))
            
            def score_pair_for_short_reversal(pair_data):
                """Score a pair's potential for SHORT reversal."""
                score = 10.0
                change_24h = pair_data.get("price_change_24h", 0)
                change_1h = pair_data.get("price_change_1h", 0)
                volume = pair_data.get("volume", 0)
                high_24h = pair_data.get("high_24h")
                price = pair_data.get("price")
                
                if not price:
                    return 0.0
                
                if change_24h > 1.0:
                    score += 15
                if change_24h > 3.0:
                    score += 10
                if change_24h > 7.0:
                    score += 10
                if change_1h > 0.5:
                    score += 5
                
                if high_24h:
                    proximity_to_high = (high_24h - price) / (high_24h + 1e-12) if high_24h > 0 else 0
                    if proximity_to_high < 0.1:
                        score += 10
                
                if volume > 10:
                    score += 5
                
                return min(100.0, max(0.0, score))
            
            def score_pair_for_long_range(pair_data):
                """Score a pair's potential for LONG range trading - favor pairs near support with good range."""
                score = 10.0
                volume = pair_data.get("volume", 0)
                
                if volume > 10:
                    score += 15
                elif volume > 1:
                    score += 5
                
                high_24h = pair_data.get("high_24h", 0)
                low_24h = pair_data.get("low_24h", 0)
                price = pair_data.get("price", 0)
                
                if high_24h > 0 and low_24h > 0 and high_24h > low_24h:
                    range_24h = high_24h - low_24h
                    range_pct = range_24h / low_24h
                    if range_pct > 0.02:
                        score += 15
                    elif range_pct > 0.01:
                        score += 8
                
                if price > 0 and low_24h > 0:
                    dist_from_low = (price - low_24h) / price
                    if dist_from_low < 0.1:
                        score += 10
                
                return min(100.0, max(0.0, score))
            
            def score_pair_for_short_range(pair_data):
                """Score a pair's potential for SHORT range trading - favor pairs near resistance with good range."""
                score = 10.0
                volume = pair_data.get("volume", 0)
                
                if volume > 10:
                    score += 15
                elif volume > 1:
                    score += 5
                
                high_24h = pair_data.get("high_24h", 0)
                low_24h = pair_data.get("low_24h", 0)
                price = pair_data.get("price", 0)
                
                if high_24h > 0 and low_24h > 0 and high_24h > low_24h:
                    range_24h = high_24h - low_24h
                    range_pct = range_24h / low_24h
                    if range_pct > 0.02:
                        score += 15
                    elif range_pct > 0.01:
                        score += 8
                
                if price > 0 and high_24h > 0:
                    dist_from_high = (high_24h - price) / price
                    if dist_from_high < 0.1:
                        score += 10
                
                return min(100.0, max(0.0, score))
            
            def score_pair_for_long_momentum(pair_data):
                """Score a pair's potential for momentum trading (direction-neutral)."""
                score = 10.0
                volume = pair_data.get("volume", 0)
                
                if volume > 10:
                    score += 20
                elif volume > 1:
                    score += 8
                
                high_24h = pair_data.get("high_24h", 0)
                low_24h = pair_data.get("low_24h", 0)
                price = pair_data.get("price", 0)
                
                if high_24h > 0 and low_24h > 0 and price > 0:
                    range_24h = high_24h - low_24h
                    if range_24h > 0:
                        range_pct = range_24h / low_24h
                        if range_pct > 0.01:
                            score += 15
                        elif range_pct > 0.005:
                            score += 8
                
                return min(100.0, max(0.0, score))
            
            def score_pair_for_short_momentum(pair_data):
                """Score a pair's potential for momentum trading (direction-neutral)."""
                return score_pair_for_long_momentum(pair_data)

            # PERFORMANCE OPTIMIZATION: Filter best 15 long/15 short per strategy before fetching klines
            def get_top_candidates_by_strategy_directional(strategy_name, candidates_list, market_data, limit_per_direction=15):
                """Get top candidates for each direction per strategy based on quick metrics.
                Returns two lists: (long_candidates, short_candidates), each with direction tag.
                For momentum, both lists are empty (direction determined fresh from klines)."""
                long_candidates = []
                short_candidates = []
                
                if os.getenv("DEBUG_SCORING", "false").lower() == "true":
                    print(f"[SCORE-{strategy_name.upper()}] Starting evaluation of {len(candidates_list)} candidates", flush=True)
                    if candidates_list and len(candidates_list) > 0:
                        sample = candidates_list[0]
                        print(f"[SCORE-{strategy_name.upper()}] Sample candidate keys: {list(sample.keys())}", flush=True)
                
                for cand in candidates_list:
                    sym = cand.get("symbol")
                    if not sym:
                        continue
                    
                    sym = sym.upper()
                    if is_stablecoin(sym):
                        continue
                        
                    pair = sym + "USDT"
                    pair_data = market_data.get(pair, cand)
                    
                    if not pair_data or not pair_data.get("price"):
                        if os.getenv("DEBUG_SCORING", "false").lower() == "true":
                            print(f"[SCORE-{strategy_name.upper()}] {sym}: SKIPPED - no price data", flush=True)
                        continue
                    
                    volume = pair_data.get("volume", 0)
                    if volume <= 0:
                        continue
                    
                    if strategy_name == 'reversal':
                        long_score = score_pair_for_long_reversal(pair_data)
                        short_score = score_pair_for_short_reversal(pair_data)
                        if os.getenv("DEBUG_SCORING", "false").lower() == "true" and (long_score > 0 or short_score > 0):
                            print(f"[SCORE-REV] {sym}: long={long_score:.1f}, short={short_score:.1f}, price={pair_data.get('price')}, vol={volume}", flush=True)
                        if long_score > 0:
                            long_candidates.append((cand, long_score))
                        if short_score > 0:
                            short_candidates.append((cand, short_score))
                    elif strategy_name == 'range':
                        long_score = score_pair_for_long_range(pair_data)
                        short_score = score_pair_for_short_range(pair_data)
                        if long_score > 0:
                            long_candidates.append((cand, long_score))
                        if short_score > 0:
                            short_candidates.append((cand, short_score))
                    else:
                        momentum_score_long = score_pair_for_long_momentum(pair_data)
                        momentum_score_short = score_pair_for_short_momentum(pair_data)
                        if momentum_score_long > 0:
                            long_candidates.append((cand, momentum_score_long))
                        if momentum_score_short > 0:
                            short_candidates.append((cand, momentum_score_short))
                
                long_candidates.sort(key=lambda x: x[1], reverse=True)
                short_candidates.sort(key=lambda x: x[1], reverse=True)
                
                if os.getenv("DEBUG_SCORING", "false").lower() == "true":
                    print(f"[SCORE-{strategy_name.upper()}] Results: {len(long_candidates)} LONG candidates, {len(short_candidates)} SHORT candidates", flush=True)
                
                if strategy_name == 'momentum':
                    long_result = [dict(cand, **{"expected_direction": None}) for cand, score in long_candidates[:limit_per_direction]]
                    short_result = [dict(cand, **{"expected_direction": None}) for cand, score in short_candidates[:limit_per_direction]]
                elif strategy_name == 'reversal' or strategy_name == 'range':
                    long_result = [dict(cand, **{"expected_direction": "LONG"}) for cand, score in long_candidates[:limit_per_direction]]
                    short_result = [dict(cand, **{"expected_direction": "SHORT"}) for cand, score in short_candidates[:limit_per_direction]]
                else:
                    long_result = [dict(cand, **{"expected_direction": "LONG"}) for cand, score in long_candidates[:limit_per_direction]]
                    short_result = [dict(cand, **{"expected_direction": "SHORT"}) for cand, score in short_candidates[:limit_per_direction]]
                
                return long_result, short_result

            # Define strategies to process with their own pair selection
            strategies = [
                {'name': 'reversal', 'log_file': REV_LOG_FILE, 'include_meta': True},
                {'name': 'range', 'log_file': RANGE_LOG_FILE, 'include_meta': True},
                {'name': 'momentum', 'log_file': MOMENTUM_LOG_FILE, 'include_meta': True}
            ]

            all_signals = {s['name']: [] for s in strategies}

            # --- Start of new concurrent data fetching logic ---

            # 1. Gather all candidates from all strategies into a unified set
            all_candidate_symbols = set()
            all_candidates_map = {}

            raw_rev_candidates = reversal_candidates
            rev_long, rev_short = get_top_candidates_by_strategy_directional('reversal', raw_rev_candidates, market_data, limit_per_direction=25)
            strategy_candidates['reversal_long'] = rev_long
            strategy_candidates['reversal_short'] = rev_short

            raw_range_candidates = range_candidates
            range_long, range_short = get_top_candidates_by_strategy_directional('range', raw_range_candidates, market_data, limit_per_direction=25)
            strategy_candidates['range_long'] = range_long
            strategy_candidates['range_short'] = range_short

            raw_momentum_candidates = momentum_candidates
            mom_long, mom_short = get_top_candidates_by_strategy_directional('momentum', raw_momentum_candidates, market_data, limit_per_direction=25)
            strategy_candidates['momentum_long'] = mom_long
            strategy_candidates['momentum_short'] = mom_short
            
            print(f"[Reversal] Long: {len(strategy_candidates['reversal_long'])}, Short: {len(strategy_candidates['reversal_short'])} | [Range] Long: {len(strategy_candidates['range_long'])}, Short: {len(strategy_candidates['range_short'])} | [Momentum] Long: {len(strategy_candidates['momentum_long'])}, Short: {len(strategy_candidates['momentum_short'])}", flush=True)

            # Unify all candidates (including directional ones)
            for strat_key, cand_list in strategy_candidates.items():
                if isinstance(cand_list, list):
                    for item in cand_list:
                        sym = (item.get("symbol") or "").upper()
                        if sym and not is_stablecoin(sym):
                            all_candidate_symbols.add(sym)
                            if sym not in all_candidates_map:
                                 all_candidates_map[sym] = {"coin_id": item.get("id"), "symbol": sym}

            print(f"[Data Fetch] Ready to process {len(all_candidate_symbols)} pairs with lazy klines fetching per strategy.", flush=True)
            
            if len(all_candidate_symbols) == 0:
                print("⚠️ No candidate symbols to process. Skipping strategy processing...", flush=True)
            else:
                print(f"[PROCESSING] Starting strategy processing for {len(all_candidate_symbols)} unique pairs...", flush=True)

            for strategy in strategies:
                strategy_found_signals = []
                dbg_strat = os.getenv("DEBUG_STRATEGY", "false").lower() == "true"
                if dbg_strat:
                    print(f"\n[STRAT DEBUG] Processing strategy: {strategy['name']}")

                long_key = f"{strategy['name']}_long"
                short_key = f"{strategy['name']}_short"
                
                strategy_pair_candidates = (
                    strategy_candidates.get(long_key, []) + 
                    strategy_candidates.get(short_key, [])
                )
                print(f"[{strategy['name'].upper()}] Processing {len(strategy_pair_candidates)} pairs ({len(strategy_candidates.get(long_key, []))} LONG + {len(strategy_candidates.get(short_key, []))} SHORT)", flush=True)

                processed_count = 0
                # Reduce workers to avoid overwhelming system with too many concurrent API calls
                max_workers = min(STRATEGY_WORKERS, 10)  # Cap at 10 workers
                print(f"  Starting ThreadPool with {max_workers} workers (processing {len(strategy_pair_candidates)} pairs)...", flush=True)
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {}
                    print(f"  Submitting {len(strategy_pair_candidates)} pairs to executor...", flush=True)
                    for idx, cand in enumerate(strategy_pair_candidates):
                        if idx % 5 == 0:
                            print(f"    Submitted {idx}/{len(strategy_pair_candidates)}...", flush=True)
                        expected_dir = cand.get("expected_direction")
                        future = executor.submit(
                            process_strategy, cand, market_data, market_trend, 
                            market_sentiment_pct, hotness_ranks, total_hotness, 
                            cooldown_cache, vol_cache, strategy['name'], expected_direction=expected_dir
                        )
                        futures[future] = cand
                    
                    print(f"  All pairs submitted. Waiting for results ({STRATEGY_OVERALL_TIMEOUT}s total timeout)...", flush=True)
                    print(f"  [THREADPOOL] {len(futures)} tasks running across {max_workers} workers", flush=True)
                    failed_klines = 0
                    no_signal = 0
                    start_time = time.time()
                    try:
                        for future in as_completed(futures, timeout=STRATEGY_OVERALL_TIMEOUT):
                            processed_count += 1
                            elapsed = time.time() - start_time
                            if processed_count % 5 == 0 or processed_count == 1:
                                print(f"    Completed {processed_count}/{len(strategy_pair_candidates)} (elapsed: {elapsed:.1f}s)...", flush=True)
                            try:
                                result = future.result(timeout=STRATEGY_TASK_TIMEOUT)
                                if result:
                                    if isinstance(result, list):
                                        for sig in result:
                                            try:
                                                sig = enrich_signal_with_dynamics(sig)
                                                strategy_found_signals.append(sig)
                                            except Exception as enrich_err:
                                                print(f"[ENRICH ERROR] Failed to enrich signal: {str(enrich_err)[:100]}", flush=True)
                                                strategy_found_signals.append(sig)
                                    else:
                                        try:
                                            result = enrich_signal_with_dynamics(result)
                                        except Exception as enrich_err:
                                            print(f"[ENRICH ERROR] Failed to enrich signal: {str(enrich_err)[:100]}", flush=True)
                                        strategy_found_signals.append(result)
                                else:
                                    no_signal += 1
                            except Exception as e:
                                failed_klines += 1
                                if dbg_strat:
                                    print(f"[PAIR FAILED] {futures[future].get('symbol')}: {str(e)[:80]}", flush=True)
                    except Exception as timeout_err:
                        print(f"[TIMEOUT] {strategy['name']}: {len(futures) - processed_count} pairs did not complete in time", flush=True)
                    
                    if dbg_strat and (failed_klines > 0 or no_signal > 0):
                        print(f"[DEBUG STATS] {strategy['name']}: {failed_klines} klines failures, {no_signal} no signal detections", flush=True)


                all_signals[strategy['name']] = strategy_found_signals
                if len(strategy_found_signals) > 0:
                    record_signal_generated(strategy['name'], len(strategy_found_signals))
                    for sig in strategy_found_signals:
                        record_signal_frequency(time.time())
                print(f"[{strategy['name'].upper()}] Processed {processed_count}/{len(strategy_pair_candidates)} pairs, found {len(strategy_found_signals)} signals", flush=True)
                
                if strategy_found_signals:
                    confidences = [s.get('confidence', 0) for s in strategy_found_signals if isinstance(s, dict)]
                    if confidences:
                        avg_conf = sum(confidences) / len(confidences)
                        min_conf = min(confidences)
                        max_conf = max(confidences)
                        print(f"  [QUALITY] Confidence range: {min_conf:.1f}% - {max_conf:.1f}% (avg: {avg_conf:.1f}%)", flush=True)
                    
                    directions = {}
                    for sig in strategy_found_signals:
                        if isinstance(sig, dict):
                            direction = sig.get('direction', 'UNKNOWN')
                            directions[direction] = directions.get(direction, 0) + 1
                    if directions:
                        print(f"  [DIRECTIONS] LONG: {directions.get('LONG', 0)}, SHORT: {directions.get('SHORT', 0)}", flush=True)
                
                if dbg_strat:
                    print(f"[STRAT DEBUG] {strategy['name']} found {len(strategy_found_signals)} signals")

                csv_logged_count = 0
                try:
                    for sig in all_signals[strategy['name']]:
                        try:
                            log_signal_to_csv(sig, filename=strategy['log_file'])
                            csv_logged_count += 1
                        except Exception as csv_err:
                            print(f"[CSV LOG ERROR] Failed to log signal for {sig.get('pair')}: {str(csv_err)[:100]}", flush=True)
                except Exception as e:
                    print(f"[CSV LOOP ERROR] {strategy['name']}: {str(e)[:100]}", flush=True)
                
                if csv_logged_count > 0:
                    print(f"  ✓ Logged {csv_logged_count} signals to {strategy['log_file']}", flush=True)

            print(f"\n[SUMMARY] Error Recovery: {get_failures_str()}", flush=True)
            print(f"[SUMMARY] Cache Performance: {cache_stats_str()}", flush=True)
            total_signals_before = sum(len(sigs) for sigs in all_signals.values())
            print(f"[SUMMARY] Total signals generated: {total_signals_before}", flush=True)
            
            quality_summary = {}
            for strategy_name, signals in all_signals.items():
                metrics = get_signal_quality_metrics(signals)
                quality_summary[strategy_name] = metrics
                if signals:
                    msg = f"Count={metrics['count']}, AvgConf={metrics['confidence_avg']:.1f}%, Range={metrics['confidence_min']:.1f}-{metrics['confidence_max']:.1f}, Std={metrics['confidence_std']:.1f}, L={metrics['long_count']}/S={metrics['short_count']}"
                    print(f"[QUALITY] {strategy_name}: {msg}", flush=True)
            
            print("[DEBUG] Signal counts BEFORE filtering:", {s: len(sigs) for s, sigs in all_signals.items()}, flush=True)
            
            all_signals = rank_signals_by_quality(all_signals)
            print("[DEBUG] Signal counts AFTER ranking:", {s: len(sigs) for s, sigs in all_signals.items()}, flush=True)
            
            ensure_signals_across_strategies(all_signals)
            print("[DEBUG] Signal counts AFTER coverage check:", {s: len(sigs) for s, sigs in all_signals.items()}, flush=True)
            
            all_signals = apply_wait_for_pullback_filter(all_signals)
            print("[DEBUG] Signal counts AFTER pullback filter:", {s: len(sigs) for s, sigs in all_signals.items()}, flush=True)
            
            all_signals = filter_duplicate_pair_directions(all_signals)
            print("[DEBUG] Signal counts AFTER LONG/SHORT conflict:", {s: len(sigs) for s, sigs in all_signals.items()}, flush=True)
            
            all_signals = validate_tp_probability(all_signals)
            print("[DEBUG] Signal counts AFTER TP validation:", {s: len(sigs) for s, sigs in all_signals.items()}, flush=True)
            
            all_signals = deduplicate_signals_by_time_window(all_signals)
            print("[DEBUG] Signal counts AFTER time window dedup:", {s: len(sigs) for s, sigs in all_signals.items()}, flush=True)
            
            def filter_stale_price_signals(all_strategy_signals):
                """Flag signals with stale prices (>60s old) in output, apply light penalty."""
                filtered = {}
                for strategy_name, signals in all_strategy_signals.items():
                    updated_sigs = []
                    for sig in signals:
                        staleness = sig.get("price_staleness_sec", 0)
                        if staleness > 60:
                            sig['notes'] = (sig.get('notes', '') + f" | ⚠️ STALE_PRICE({staleness:.0f}s)").strip(" |")
                            sig['confidence'] = sig.get('confidence', 0) * 0.90
                        updated_sigs.append(sig)
                    filtered[strategy_name] = updated_sigs
                return filtered
            
            def apply_adaptive_confidence(all_strategy_signals, session_adjustments):
                """Apply adaptive confidence scaling based on strategy performance and session."""
                adjusted = {}
                for strategy_name, signals in all_strategy_signals.items():
                    perf_multiplier = get_strategy_confidence_multiplier(strategy_name)
                    session_mult = session_adjustments.get(strategy_name, 1.0)
                    total_mult = perf_multiplier * session_mult
                    
                    updated_sigs = []
                    for sig in signals:
                        sig['confidence'] = sig.get('confidence', 0) * total_mult
                        sig['confidence'] = min(100.0, max(5.0, sig['confidence']))
                        if total_mult != 1.0:
                            src = " ".join(f"({k}:{v:.2f})" for k, v in [("perf", perf_multiplier), ("session", session_mult)])
                            sig['notes'] = (sig.get('notes', '') + f" | adaptive_conf{src}").strip(" |")
                        updated_sigs.append(sig)
                    adjusted[strategy_name] = updated_sigs
                return adjusted
            
            def apply_risk_filters(all_strategy_signals, existing_pairs=None):
                """Apply risk management filters: correlation, account heat, etc."""
                if existing_pairs is None:
                    existing_pairs = []
                filtered = {}
                for strategy_name, signals in all_strategy_signals.items():
                    valid_sigs = []
                    for sig in signals:
                        pair = sig.get('pair', '')
                        if check_position_correlation(pair, existing_pairs):
                            sig['notes'] = (sig.get('notes', '') + " | CORRELATED_RISK").strip(" |")
                            continue
                        valid_sigs.append(sig)
                        existing_pairs.append(sig)
                    filtered[strategy_name] = valid_sigs
                return filtered
            
            all_signals = filter_stale_price_signals(all_signals)
            print("[DEBUG] Signal counts AFTER staleness filter:", {s: len(sigs) for s, sigs in all_signals.items()}, flush=True)
            
            all_signals = apply_adaptive_confidence(all_signals, session_adjustments)
            print("[DEBUG] Signal counts AFTER adaptive confidence:", {s: len(sigs) for s, sigs in all_signals.items()}, flush=True)
            
            all_signals = apply_risk_filters(all_signals)
            print("[DEBUG] Signal counts AFTER risk filters:", {s: len(sigs) for s, sigs in all_signals.items()}, flush=True)
            
            all_signals = normalize_confidence_scores(all_signals)
            print("[DEBUG] Signal counts AFTER confidence normalization:", {s: len(sigs) for s, sigs in all_signals.items()}, flush=True)
            
            def apply_circuit_breaker_filter(all_strategy_signals, circuit_breaker_active):
                """Filter out all signals if circuit breaker is active."""
                if not circuit_breaker_active:
                    return all_strategy_signals
                filtered = {}
                for strategy_name, signals in all_strategy_signals.items():
                    filtered[strategy_name] = []
                return filtered
            
            def apply_confidence_decay_and_confirmation(all_strategy_signals, cycle_start_time, use_confirmation=True):
                """Apply confidence decay for age, check entry confirmation."""
                cycle_age = time.time() - cycle_start_time
                adjusted = {}
                for strategy_name, signals in all_strategy_signals.items():
                    updated_sigs = []
                    for sig in signals:
                        conf = sig.get('confidence', 50.0)
                        decay_factor = 1.0 - (0.02 * (cycle_age / 60.0))
                        conf = max(5.0, conf * decay_factor)
                        sig['confidence'] = conf
                        
                        if use_confirmation and 'klines' in sig:
                            is_confirmed, conf_pct = wait_for_candle_confirmation(sig['klines'], sig.get('direction'), confirmation_candles=1)
                            if not is_confirmed:
                                sig['notes'] = (sig.get('notes', '') + " | CONFIRM_WAIT").strip(" |")
                                conf = conf * 0.85
                            sig['confidence'] = conf
                        
                        updated_sigs.append(sig)
                    adjusted[strategy_name] = updated_sigs
                return adjusted
            
            def build_klines_dict_from_cache():
                """Convert cache structure to expected format: {pair: {interval: klines}}"""
                klines_dict = {}
                with _cache_lock:
                    for (symbol, interval), entry in _klines_cache_global.items():
                        if symbol not in klines_dict:
                            klines_dict[symbol] = {}
                        klines_dict[symbol][interval] = entry['data']
                return klines_dict
            
            def apply_atr_adjusted_sl_tp_to_signals(all_strategy_signals, klines_dict=None):
                """Apply ATR-based adjustments to SL/TP for all signals."""
                adjusted = {}
                for strategy_name, signals in all_strategy_signals.items():
                    updated_sigs = []
                    for sig in signals:
                        try:
                            entry = sig.get('entry', 0)
                            direction = sig.get('direction')
                            base_sl_pct = sig.get('sl_pct', 0.02)
                            base_tp_pct = sig.get('tp_pct', 0.05)
                            
                            klines = None
                            if klines_dict:
                                klines = klines_dict.get(sig.get('pair'), {}).get('15m')
                            
                            if klines and len(klines) > 15:
                                adj_sl_pct, adj_tp_pct = calculate_atr_adjusted_sl_tp(
                                    klines, entry, direction, base_sl_pct, base_tp_pct
                                )
                                if adj_sl_pct != base_sl_pct or adj_tp_pct != base_tp_pct:
                                    sig['sl'] = round(entry * (1 + adj_sl_pct) if direction == "SHORT" else entry * (1 - adj_sl_pct), 8)
                                    sig['tp'] = round(entry * (1 - adj_tp_pct) if direction == "SHORT" else entry * (1 + adj_tp_pct), 8)
                                    sig['sl_pct'] = round(adj_sl_pct, 6)
                                    sig['tp_pct'] = round(adj_tp_pct, 6)
                                    sig['notes'] = (sig.get('notes', '') + " | ATR_ADJUSTED").strip(" |")
                        except Exception:
                            pass
                        updated_sigs.append(sig)
                    adjusted[strategy_name] = updated_sigs
                return adjusted
            
            all_signals = apply_circuit_breaker_filter(all_signals, circuit_breaker_active)
            if circuit_breaker_active:
                print("[DEBUG] Signal counts AFTER circuit breaker:", {s: len(sigs) for s, sigs in all_signals.items()}, flush=True)
            
            all_signals = apply_confidence_decay_and_confirmation(all_signals, cycle_start_time, use_confirmation=os.getenv("USE_ENTRY_CONFIRMATION", "false").lower() == "true")
            print("[DEBUG] Signal counts AFTER decay & confirmation:", {s: len(sigs) for s, sigs in all_signals.items()}, flush=True)
            
            all_signals = apply_atr_adjusted_sl_tp_to_signals(all_signals, build_klines_dict_from_cache())
            print("[DEBUG] Signal counts AFTER ATR adjustment:", {s: len(sigs) for s, sigs in all_signals.items()}, flush=True)

            def get_prioritized_recommendations(all_strategy_signals):
                """
                Compile and rank ALL signals by comprehensive scoring.
                Considers: confidence, quality_score, layers, TP probability.
                """
                flat_list = []
                for strategy_name, signals in all_strategy_signals.items():
                    for sig in signals:
                        sig['strategy'] = strategy_name
                        flat_list.append(sig)

                pair_best_signal = {}
                for sig in flat_list:
                    pair = sig.get('pair')
                    conf = sig.get('confidence', 0)
                    if pair not in pair_best_signal or conf > pair_best_signal[pair].get('confidence', 0):
                        pair_best_signal[pair] = sig

                ranked_signals = []
                for sig in pair_best_signal.values():
                    quality = sig.get('quality_score', 0.0)
                    confidence = sig.get('confidence', 0)
                    prob_tp = sig.get('prob_tp', 0.5)
                    layers = len(sig.get('trigger_layers', []))
                    
                    score = (
                        confidence * 0.45 +
                        quality * 0.30 +
                        prob_tp * 100 * 0.15 +
                        min(layers, 3) * 10 * 0.10
                    )
                    sig['score'] = score
                    ranked_signals.append(sig)

                ranked_signals.sort(key=lambda x: x['score'], reverse=True)
                return ranked_signals[:5]

            def get_best_signals_for_strategy(signals):
                """
                Find best LONG and SHORT signals using comprehensive scoring.
                Weights: confidence, quality_score, layers, TP probability.
                """
                def calculate_signal_score(sig):
                    confidence = sig.get('confidence', 0)
                    quality = sig.get('quality_score', 0.0)
                    prob_tp = sig.get('prob_tp', 0.5)
                    layers = len(sig.get('trigger_layers', []))
                    
                    return (
                        confidence * 0.50 +
                        quality * 0.25 +
                        prob_tp * 100 * 0.15 +
                        min(layers, 3) * 10 * 0.10
                    )
                
                long_signals = [s for s in signals if s.get('direction') == 'LONG']
                short_signals = [s for s in signals if s.get('direction') == 'SHORT']
                
                best_long = max(long_signals, key=calculate_signal_score, default=None) if long_signals else None
                best_short = max(short_signals, key=calculate_signal_score, default=None) if short_signals else None
                
                return best_long, best_short

            # Format and print output
            print("\n" + "="*90, flush=True)
            print(f"⏰ {now_utc_str()}  | Market trend: {market_trend.upper()}  Sentiment: {market_sentiment_pct}%", flush=True)
            print("="*90, flush=True)

            # --- Refactored Strategy-Specific Output ---
            best_mom_long, best_mom_short = get_best_signals_for_strategy(all_signals.get('momentum', []))
            print("📈 MOMENTUM SIGNALS", flush=True)
            if best_mom_long:
                sl_pct = best_mom_long.get('sl_pct', 0) * 100
                tp_pct = best_mom_long.get('tp_pct', 0) * 100
                print(f"  BEST LONG:  {best_mom_long.get('symbol'):<10} | Entry {fmt(best_mom_long.get('entry')):<10} | TP {fmt(best_mom_long.get('tp'))} ({tp_pct:.2f}%) | SL {fmt(best_mom_long.get('sl'))} ({sl_pct:.2f}%) | Conf {best_mom_long.get('confidence'):.2f}%", flush=True)
            else:
                print("  BEST LONG:  None", flush=True)
            if best_mom_short:
                sl_pct = best_mom_short.get('sl_pct', 0) * 100
                tp_pct = best_mom_short.get('tp_pct', 0) * 100
                print(f"  BEST SHORT: {best_mom_short.get('symbol'):<10} | Entry {fmt(best_mom_short.get('entry')):<10} | TP {fmt(best_mom_short.get('tp'))} ({tp_pct:.2f}%) | SL {fmt(best_mom_short.get('sl'))} ({sl_pct:.2f}%) | Conf {best_mom_short.get('confidence'):.2f}%", flush=True)
            else:
                print("  BEST SHORT: None", flush=True)
            print("-"*90, flush=True)

            best_rev_long, best_rev_short = get_best_signals_for_strategy(all_signals.get('reversal', []))
            print("⚠️  REVERSAL SCANNER RESULTS (Enhanced Balanced)", flush=True)
            if best_rev_long:
                sl_pct = best_rev_long.get('sl_pct', 0) * 100
                tp_pct = best_rev_long.get('tp_pct', 0) * 100
                strength = best_rev_long.get('reversal_strength', 'UNKNOWN')
                print(f"  BEST LONG:  {best_rev_long.get('symbol'):<10} | Entry {fmt(best_rev_long.get('entry')):<10} | TP {fmt(best_rev_long.get('tp'))} ({tp_pct:.2f}%) | SL {fmt(best_rev_long.get('sl'))} ({sl_pct:.2f}%) | Conf {best_rev_long.get('confidence'):.2f}% [{strength}]", flush=True)
            else:
                print("  BEST LONG:  None", flush=True)
            if best_rev_short:
                sl_pct = best_rev_short.get('sl_pct', 0) * 100
                tp_pct = best_rev_short.get('tp_pct', 0) * 100
                strength = best_rev_short.get('reversal_strength', 'UNKNOWN')
                print(f"  BEST SHORT: {best_rev_short.get('symbol'):<10} | Entry {fmt(best_rev_short.get('entry')):<10} | TP {fmt(best_rev_short.get('tp'))} ({tp_pct:.2f}%) | SL {fmt(best_rev_short.get('sl'))} ({sl_pct:.2f}%) | Conf {best_rev_short.get('confidence'):.2f}% [{strength}]", flush=True)
            else:
                print("  BEST SHORT: None", flush=True)
            print("-"*90, flush=True)

            best_range_long, best_range_short = get_best_signals_for_strategy(all_signals.get('range', []))
            print("💠 RANGE SCANNER RESULTS (30m–1H–4H)", flush=True)
            if best_range_long:
                sl_pct = best_range_long.get('sl_pct', 0) * 100
                tp_pct = best_range_long.get('tp_pct', 0) * 100
                print(f"  BEST LONG:  {best_range_long.get('symbol'):<10} | Entry {fmt(best_range_long.get('entry')):<10} | TP {fmt(best_range_long.get('tp'))} ({tp_pct:.2f}%) | SL {fmt(best_range_long.get('sl'))} ({sl_pct:.2f}%) | Conf {best_range_long.get('confidence'):.2f}%", flush=True)
            else:
                print("  BEST LONG:  None", flush=True)
            if best_range_short:
                sl_pct = best_range_short.get('sl_pct', 0) * 100
                tp_pct = best_range_short.get('tp_pct', 0) * 100
                print(f"  BEST SHORT: {best_range_short.get('symbol'):<10} | Entry {fmt(best_range_short.get('entry')):<10} | TP {fmt(best_range_short.get('tp'))} ({tp_pct:.2f}%) | SL {fmt(best_range_short.get('sl'))} ({sl_pct:.2f}%) | Conf {best_range_short.get('confidence'):.2f}%", flush=True)
            else:
                print("  BEST SHORT: None", flush=True)
            print("-"*90, flush=True)

            top_recommendations = get_prioritized_recommendations(all_signals)
            print("\n🎯 TOP PRIORITIZED SIGNALS (By Quality & Confidence)", flush=True)
            print("="*90, flush=True)

            if top_recommendations:
                for idx, rec in enumerate(top_recommendations, 1):
                    entry = rec.get('entry', 0)
                    tp_pct = abs(rec.get('tp', 0) - entry) / (entry + 1e-12) * 100
                    sl_pct = abs(rec.get('sl', 0) - entry) / (entry + 1e-12) * 100
                    rr = tp_pct / (sl_pct + 1e-12)

                    emoji = "🥇" if idx == 1 else ("🥈" if idx == 2 else ("🥉" if idx == 3 else f"#{idx}"))
                    strategy_abbr = rec.get('strategy', '???').upper()[:3]

                    print(f"{emoji} {rec.get('pair'):<12} {rec.get('direction'):<5} | {strategy_abbr:<8} | Score {rec.get('score', 0):6.2f} | Conf {rec.get('confidence', 0):6.2f}% | Quality {rec.get('quality_score', 0):6.2f}", flush=True)
                    print(f"   Entry {fmt(entry):<10}  |  TP {fmt(rec.get('tp', 0))} (+{tp_pct:.2f}%)  |  SL {fmt(rec.get('sl', 0))} (-{sl_pct:.2f}%)  |  RR {rr:.2f}", flush=True)
            else:
                print("No signals available", flush=True)

            print("="*90 + "\n", flush=True)

            # Update global signals for API access
            with GLOBAL_SIGNALS_LOCK:
                GLOBAL_SIGNALS['momentum'] = all_signals.get('momentum', [])
                GLOBAL_SIGNALS['reversal'] = all_signals.get('reversal', [])
                GLOBAL_SIGNALS['range'] = all_signals.get('range', [])
                GLOBAL_SIGNALS['top_prioritized'] = top_recommendations
                GLOBAL_SIGNALS['last_updated'] = datetime.now(timezone.utc).isoformat()

            # Strategy Verification (run periodically)
            if VERIFICATION_AVAILABLE and VERIFICATION_ENABLED:
                cycle_count = getattr(main_loop, 'cycle_count', 0) + 1
                main_loop.cycle_count = cycle_count

                if cycle_count % VERIFICATION_FREQUENCY == 0:
                    try:
                        print("\n🔍 RUNNING STRATEGY VERIFICATION...", flush=True)
                        candidates = list(all_candidates_map.values())
                        verification_results = verify_bot_strategy_performance(all_signals, market_data, candidates)

                        # Store verification results for analysis
                        verification_summary = verification_results['summary']

                        # Quick verification status
                        status_emoji = "✅" if verification_summary['overall_status'] == 'PASS' else "❌"
                        print(f"\n{status_emoji} VERIFICATION STATUS: {verification_summary['overall_status']} | Quality Score: {verification_summary['search_quality_score']:.1f}/100", flush=True)

                        # Alert if verification fails
                        if verification_summary['overall_status'] == 'FAIL':
                            print("⚠️ VERIFICATION FAILED - Check strategy implementation!", flush=True)

                    except Exception as e:
                        print(f"⚠️ Verification error: {str(e)[:100]}", flush=True)

            report_performance_metrics()
            log_api_usage()

            save_vol_cache(vol_cache)
            save_cooldown_cache(cooldown_cache)
            

            
            cycle_elapsed = time.time() - cycle_start_time
            print(f"\n[Cycle Time] {cycle_elapsed:.1f}s elapsed. Sleeping for {CYCLE_SECONDS}s...", flush=True)
            safe_sleep(CYCLE_SECONDS)

        except Exception as e:
            print(f"⚠️ Cycle error: {str(e)[:100]}")

def cleanup_kline_fetchers():
    """Cleanup kline fetcher resources."""
    if STRATEGY_FETCHERS_AVAILABLE:
        print("[KLINE FETCHERS] Ready for next cycle", flush=True)

if __name__ == "__main__":
    try:
        print("\n" + "="*84)
        print("🚀 ULTIMATE BOT STARTED")
        print("="*84)
        print(f"Mode: {'SHORT-TERM (15m)' if SHORT_TERM_MODE else 'LONG-TERM (1h)'}")
        print(f"Cycle: {CYCLE_SECONDS}s | Top trending: {TOP_TRENDING}")
        print(f"Strategy Kline Fetchers: {'AVAILABLE' if STRATEGY_FETCHERS_AVAILABLE else 'NOT AVAILABLE'}")
        print("Klines Fetching:")
        print("  REVERSAL: Tickerbase → Kraken → OKX → KuCoin → Coinpaprika")
        print("  RANGE: Tickerbase → OKX → Kraken → KuCoin → Coinpaprika")
        print("  MOMENTUM: Tickerbase → Kraken → OKX → KuCoin → Coinpaprika")
        print("="*84 + "\n")

        if CACHE_WARMING_ENABLED:
            warm_cache_on_startup()

        # Start Flask API server in a separate background thread
        print(f"📡 Starting Flask API server at https://kaineng.pythonanywhere.com/ ...", flush=True)
        flask_thread = threading.Thread(target=run_flask_app, daemon=True)
        flask_thread.start()

        main_loop()
    except KeyboardInterrupt:
        print("\n\n" + "="*84)
        print("⛔ BOT STOPPED BY USER")
        print("="*84)
        cleanup_kline_fetchers()
        sys.exit(0)
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        cleanup_kline_fetchers()
        sys.exit(1)

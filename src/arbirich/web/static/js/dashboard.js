// Dashboard-specific JavaScript

// Real-time updates via WebSockets
function setupWebSockets() {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${window.location.host}/ws/dashboard`;

    const ws = new WebSocket(wsUrl);

    ws.onopen = function () {
        console.log('WebSocket connection established');
    };

    ws.onmessage = function (event) {
        const data = JSON.parse(event.data);

        // Update UI based on received data
        if (data.type === 'stats') {
            updateStats(data.stats);
        }
    };

    ws.onclose = function () {
        console.log('WebSocket connection closed');
        // Try to reconnect after a delay
        setTimeout(setupWebSockets, 5000);
    };

    ws.onerror = function (error) {
        console.error('WebSocket error:', error);
    };
}

// Function to update statistics
function updateStats(stats) {
    if (stats.total_opportunities !== undefined) {
        const element = document.getElementById('total-opportunities');
        if (element) element.textContent = stats.total_opportunities;
    }

    if (stats.total_executions !== undefined) {
        const element = document.getElementById('total-executions');
        if (element) element.textContent = stats.total_executions;
    }

    // Calculate success rate
    if (stats.total_opportunities > 0) {
        const successRate = ((stats.total_executions / stats.total_opportunities) * 100).toFixed(2);
        const element = document.getElementById('success-rate');
        if (element) element.textContent = `${successRate}%`;
    }
}

// Initialize WebSockets when the page loads
document.addEventListener('DOMContentLoaded', function () {
    // Only set up WebSockets on dashboard page
    if (window.location.pathname === '/dashboard') {
        setupWebSockets();
        console.log('Dashboard WebSocket initialized');
    }

    // Add debugging info to help diagnose issues
    console.info('Page loaded: ' + window.location.pathname);
});

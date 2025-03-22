// Dashboard-specific JavaScript

// Real-time updates via WebSockets
function setupWebSockets() {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${window.location.host}/ws/dashboard`;

    const ws = new WebSocket(wsUrl);

    ws.onopen = function () {
        console.log('WebSocket connection established');
        // Request initial data
        ws.send(JSON.stringify({ action: 'get_data' }));
    };

    ws.onmessage = function (event) {
        const data = JSON.parse(event.data);
        console.log('Received data:', data);

        // Update UI based on received data
        if (data.type === 'stats') {
            updateStats(data.stats);
        }

        if (data.type === 'opportunity') {
            updateOpportunityTable(data.opportunity);
            updateOpportunityChart(data.opportunity);
        }

        if (data.type === 'execution') {
            updateExecutionTable(data.execution);
        }

        if (data.type === 'full_update') {
            if (data.stats) updateStats(data.stats);
            if (data.opportunities) updateOpportunityTableFull(data.opportunities);
            if (data.executions) updateExecutionTableFull(data.executions);
            if (data.strategies) updateStrategyLeaderboard(data.strategies);
            if (data.chart_data) updateChartFull(data.chart_data);
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

    // Keep connection alive with ping
    setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ action: 'ping' }));
        }
    }, 30000);

    return ws;
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

// Function to add a new opportunity to the table
function updateOpportunityTable(opportunity) {
    const table = document.getElementById('opportunitiesTable');
    if (!table) return;

    const tbody = table.querySelector('tbody');
    if (!tbody) return;

    // Create new row
    const row = document.createElement('tr');
    row.classList.add('border-t', 'new-row-highlight');

    // Format date
    let dateStr = 'N/A';
    if (opportunity.created_at) {
        if (typeof opportunity.created_at === 'string') {
            dateStr = new Date(opportunity.created_at).toLocaleString();
        } else {
            dateStr = opportunity.created_at.toLocaleString();
        }
    }

    // Format ID
    const idDisplay = opportunity.id ? (opportunity.id.substring(0, 8) + '...') : 'N/A';

    // Add cell content
    row.innerHTML = `
        <td class="px-4 py-2">${idDisplay}</td>
        <td class="px-4 py-2">${opportunity.strategy || 'N/A'}</td>
        <td class="px-4 py-2">${opportunity.pair || 'N/A'}</td>
        <td class="px-4 py-2 ${parseFloat(opportunity.profit_percent) > 0 ? 'text-green-600' : 'text-red-600'}">
            ${parseFloat(opportunity.profit_percent).toFixed(4)}%
        </td>
        <td class="px-4 py-2">${dateStr}</td>
    `;

    // Add to the beginning of the table
    if (tbody.firstChild) {
        tbody.insertBefore(row, tbody.firstChild);
    } else {
        tbody.appendChild(row);
    }

    // Remove the last row if more than 5 rows
    while (tbody.children.length > 5) {
        tbody.removeChild(tbody.lastChild);
    }

    // Animate the new row
    setTimeout(() => {
        row.classList.remove('new-row-highlight');
    }, 3000);
}

// Update the entire opportunity table with new data
function updateOpportunityTableFull(opportunities) {
    const table = document.getElementById('opportunitiesTable');
    if (!table) return;

    const tbody = table.querySelector('tbody');
    if (!tbody) return;

    // Clear the table
    tbody.innerHTML = '';

    // Add all opportunities
    opportunities.forEach(opportunity => {
        const row = document.createElement('tr');
        row.classList.add('border-t');

        // Format date
        let dateStr = 'N/A';
        if (opportunity.created_at) {
            if (typeof opportunity.created_at === 'string') {
                dateStr = new Date(opportunity.created_at).toLocaleString();
            } else {
                dateStr = opportunity.created_at.toLocaleString();
            }
        }

        // Format ID
        const idDisplay = opportunity.id ? (opportunity.id.substring(0, 8) + '...') : 'N/A';

        // Add cell content
        row.innerHTML = `
            <td class="px-4 py-2">${idDisplay}</td>
            <td class="px-4 py-2">${opportunity.strategy || 'N/A'}</td>
            <td class="px-4 py-2">${opportunity.pair || 'N/A'}</td>
            <td class="px-4 py-2 ${parseFloat(opportunity.profit_percent) > 0 ? 'text-green-600' : 'text-red-600'}">
                ${parseFloat(opportunity.profit_percent).toFixed(4)}%
            </td>
            <td class="px-4 py-2">${dateStr}</td>
        `;

        tbody.appendChild(row);
    });
}

// Update the chart with new data
function updateOpportunityChart(opportunity) {
    if (!window.opportunitiesChart) {
        console.warn('Chart not initialized');
        return;
    }

    // Format timestamp
    let timeStr = new Date().toLocaleTimeString();
    if (opportunity.created_at) {
        if (typeof opportunity.created_at === 'string') {
            timeStr = new Date(opportunity.created_at).toLocaleTimeString();
        } else {
            timeStr = opportunity.created_at.toLocaleTimeString();
        }
    }

    // Add the new data point
    window.opportunitiesChart.data.labels.unshift(timeStr);
    window.opportunitiesChart.data.datasets[0].data.unshift(parseFloat(opportunity.profit_percent));

    // Keep only the last 10 points
    if (window.opportunitiesChart.data.labels.length > 10) {
        window.opportunitiesChart.data.labels.pop();
        window.opportunitiesChart.data.datasets[0].data.pop();
    }

    // Update the chart
    window.opportunitiesChart.update();
}

// Update the entire chart with new data
function updateChartFull(chartData) {
    if (!window.opportunitiesChart) {
        console.warn('Chart not initialized');
        return;
    }

    window.opportunitiesChart.data.labels = chartData.labels;
    window.opportunitiesChart.data.datasets[0].data = chartData.data;
    window.opportunitiesChart.update();
}

// Function to add a new execution to the table
function updateExecutionTable(execution) {
    const table = document.getElementById('executionsTable');
    if (!table) return;

    const tbody = table.querySelector('tbody');
    if (!tbody) return;

    // Create new row
    const row = document.createElement('tr');
    row.classList.add('border-t', 'border-dark-element', 'hover:bg-dark-element', 'new-row-highlight');

    // Format date
    let dateStr = 'N/A';
    if (execution.created_at) {
        if (typeof execution.created_at === 'string') {
            dateStr = new Date(execution.created_at).toLocaleString();
        } else {
            dateStr = execution.created_at.toLocaleString();
        }
    }

    // Format IDs
    const idDisplay = execution.id ? (execution.id.substring(0, 8) + '...') : 'N/A';
    const oppIdDisplay = execution.opportunity_id && execution.opportunity_id !== "None" ?
        (execution.opportunity_id.substring(0, 8) + '...') : 'N/A';

    // Status class
    let statusClass = 'bg-yellow-900 text-yellow-300';
    if (execution.status === 'completed') {
        statusClass = 'bg-green-900 text-profit-green';
    } else if (execution.status === 'failed') {
        statusClass = 'bg-red-900 text-loss-red';
    }

    // Add cell content
    row.innerHTML = `
        <td class="px-4 py-2 font-mono">${idDisplay}</td>
        <td class="px-4 py-2 font-mono">${oppIdDisplay}</td>
        <td class="px-4 py-2">${execution.strategy || 'N/A'}</td>
        <td class="px-4 py-2">
            <span class="px-2 py-1 rounded-full text-xs font-semibold ${statusClass}">
                ${execution.status || 'pending'}
            </span>
        </td>
        <td class="px-4 py-2 ${parseFloat(execution.actual_profit) > 0 ? 'text-profit-green' : 'text-loss-red'}">
            ${parseFloat(execution.actual_profit).toFixed(4)}
        </td>
        <td class="px-4 py-2 text-gray-300">${dateStr}</td>
    `;

    // Add to the beginning of the table
    if (tbody.firstChild) {
        tbody.insertBefore(row, tbody.firstChild);
    } else {
        tbody.appendChild(row);
    }

    // Remove the last row if more than 5 rows
    while (tbody.children.length > 5) {
        tbody.removeChild(tbody.lastChild);
    }

    // Animate the new row
    setTimeout(() => {
        row.classList.remove('new-row-highlight');
    }, 3000);
}

// Update the execution table with full data
function updateExecutionTableFull(executions) {
    const table = document.getElementById('executionsTable');
    if (!table) return;

    const tbody = table.querySelector('tbody');
    if (!tbody) return;

    // Clear the table
    tbody.innerHTML = '';

    // Add all executions
    executions.forEach(execution => {
        const row = document.createElement('tr');
        row.classList.add('border-t', 'border-dark-element', 'hover:bg-dark-element');

        // Format date
        let dateStr = 'N/A';
        if (execution.created_at) {
            if (typeof execution.created_at === 'string') {
                dateStr = new Date(execution.created_at).toLocaleString();
            } else {
                dateStr = execution.created_at.toLocaleString();
            }
        }

        // Format IDs
        const idDisplay = execution.id ? (execution.id.substring(0, 8) + '...') : 'N/A';
        const oppIdDisplay = execution.opportunity_id && execution.opportunity_id !== "None" ?
            (execution.opportunity_id.substring(0, 8) + '...') : 'N/A';

        // Status class
        let statusClass = 'bg-yellow-900 text-yellow-300';
        if (execution.status === 'completed') {
            statusClass = 'bg-green-900 text-profit-green';
        } else if (execution.status === 'failed') {
            statusClass = 'bg-red-900 text-loss-red';
        }

        // Add cell content
        row.innerHTML = `
            <td class="px-4 py-2 font-mono">${idDisplay}</td>
            <td class="px-4 py-2 font-mono">${oppIdDisplay}</td>
            <td class="px-4 py-2">${execution.strategy || 'N/A'}</td>
            <td class="px-4 py-2">
                <span class="px-2 py-1 rounded-full text-xs font-semibold ${statusClass}">
                    ${execution.status || 'pending'}
                </span>
            </td>
            <td class="px-4 py-2 ${parseFloat(execution.actual_profit) > 0 ? 'text-profit-green' : 'text-loss-red'}">
                ${parseFloat(execution.actual_profit).toFixed(4)}
            </td>
            <td class="px-4 py-2 text-gray-300">${dateStr}</td>
        `;

        tbody.appendChild(row);
    });
}

// Update the strategy leaderboard with full data
function updateStrategyLeaderboard(strategies) {
    const table = document.getElementById('strategyLeaderboard');
    if (!table) return;

    const tbody = table.querySelector('tbody');
    if (!tbody) return;

    // Clear the table
    tbody.innerHTML = '';

    // Add all strategies
    strategies.forEach(strategy => {
        const row = document.createElement('tr');
        row.classList.add('border-t', 'hover:bg-gray-50');

        // Calculate success rate
        let successRate = '0%';
        if (strategy.trade_count > 0) {
            const rate = (strategy.total_profit / (strategy.total_profit + strategy.total_loss)) * 100;
            successRate = `${rate.toFixed(1)}%`;
        }

        // Add cell content
        row.innerHTML = `
            <td class="px-4 py-2 font-medium">${strategy.name}</td>
            <td class="px-4 py-2 text-right text-green-600">${parseFloat(strategy.total_profit).toFixed(4)}</td>
            <td class="px-4 py-2 text-right text-red-600">${parseFloat(strategy.total_loss).toFixed(4)}</td>
            <td class="px-4 py-2 text-right ${parseFloat(strategy.net_profit) > 0 ? 'text-green-600' : 'text-red-600'}">
                ${parseFloat(strategy.net_profit).toFixed(4)}
            </td>
            <td class="px-4 py-2 text-right">${strategy.trade_count}</td>
            <td class="px-4 py-2 text-right">${successRate}</td>
        `;

        tbody.appendChild(row);
    });
}

// Initialize WebSockets and charts when the page loads
document.addEventListener('DOMContentLoaded', function () {
    // Initialize Chart.js instance
    const chartCanvas = document.getElementById('opportunitiesChart');
    if (chartCanvas) {
        const ctx = chartCanvas.getContext('2d');
        window.opportunitiesChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Profit Percentage',
                    data: [],
                    borderColor: 'rgb(59, 130, 246)',
                    backgroundColor: 'rgba(59, 130, 246, 0.2)',
                    fill: true,
                }]
            },
            options: {
                responsive: true,
                scales: {
                    y: {
                        beginAtZero: false
                    }
                }
            }
        });
    }

    // Only set up WebSockets on dashboard page
    if (window.location.pathname === '/dashboard') {
        const ws = setupWebSockets();
        window.dashboardWs = ws;
        console.log('Dashboard WebSocket initialized');

        // Set up UI refresh button
        const refreshBtn = document.getElementById('refresh-dashboard');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', function () {
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ action: 'get_data' }));
                } else {
                    window.dashboardWs = setupWebSockets();
                }
            });
        }
    }

    // Enable auto-update every 30 seconds
    window.autoUpdateInterval = setInterval(() => {
        if (window.location.pathname === '/dashboard' && window.dashboardWs && window.dashboardWs.readyState === WebSocket.OPEN) {
            console.log('Auto-updating dashboard...');
            window.dashboardWs.send(JSON.stringify({ action: 'get_data' }));
        }
    }, 30000);

    // Add debugging info to help diagnose issues
    console.info('Page loaded: ' + window.location.pathname);
});

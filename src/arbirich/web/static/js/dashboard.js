/**
 * Dashboard functionality for ArbiRich platform
 */

document.addEventListener('DOMContentLoaded', function () {
    console.log("Dashboard.js loaded successfully");

    // Initialize refresh indicator
    initializeRefreshIndicator();

    // Set up auto-refresh
    setupAutoRefresh();

    // Setup event listeners
    setupEventListeners();

    // Initialize charts if Chart.js is available
    if (typeof Chart !== 'undefined') {
        initializeProfitChart();
    } else {
        console.warn("Chart.js not loaded");
    }
});

function initializeRefreshIndicator() {
    const indicator = document.getElementById('refresh-indicator');
    if (indicator) {
        updateRefreshIndicator(indicator, 30);
    }
}

function updateRefreshIndicator(indicator, seconds) {
    // Clear any existing content
    indicator.innerHTML = '';

    // Create the progress bar
    const progressBar = document.createElement('div');
    progressBar.className = 'h-2 w-16 bg-gray-800 rounded overflow-hidden';

    // Create the inner progress
    const progress = document.createElement('div');
    progress.className = 'h-full bg-dollar-green transition-all duration-1000';
    progress.style.width = '100%';

    // Animate the progress bar to shrink
    setTimeout(() => {
        progress.style.width = '0%';
        progress.style.transitionDuration = `${seconds}s`;
        progress.style.transitionTimingFunction = 'linear';
    }, 50);

    // Add to DOM
    progressBar.appendChild(progress);
    indicator.appendChild(progressBar);
}

function setupAutoRefresh() {
    // Auto refresh the page every 30 seconds
    setTimeout(() => {
        // Only refresh if user hasn't interacted with the page in the last 5 seconds
        if ((Date.now() - window.lastInteraction || 0) > 5000) {
            window.location.reload();
        } else {
            // If user recently interacted, try again in 5 seconds
            setTimeout(() => {
                window.location.reload();
            }, 5000);
        }
    }, 30000);
}

function setupEventListeners() {
    // Track last interaction time
    window.lastInteraction = Date.now();
    ['click', 'keydown', 'mousemove', 'scroll'].forEach(event => {
        document.addEventListener(event, () => {
            window.lastInteraction = Date.now();
        });
    });
}

function initializeProfitChart() {
    const profitChartElement = document.getElementById('profitChart');
    if (!profitChartElement) {
        console.warn("Profit chart element not found");
        return;
    }

    // Get chart data from global variables (set in the template)
    const chartLabels = window.chartLabels || [];
    const chartValues = window.chartValues || [];

    console.log(`Initializing profit chart with ${chartLabels.length} data points`);

    try {
        new Chart(profitChartElement, {
            type: 'line',
            data: {
                labels: chartLabels,
                datasets: [{
                    label: 'Cumulative Profit',
                    data: chartValues,
                    fill: false,
                    borderColor: '#85BB65',  // Money green
                    tension: 0.1
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            label: function (context) {
                                return '$' + context.raw.toFixed(2);
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        ticks: {
                            callback: function (value) {
                                return '$' + value;
                            }
                        }
                    }
                }
            }
        });
        console.log("Profit chart initialized");
    } catch (e) {
        console.error("Error initializing chart:", e);
    }
}

// Format currency for display
function formatCurrency(amount) {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    }).format(amount);
}

// Format percentage for display
function formatPercent(percent) {
    return new Intl.NumberFormat('en-US', {
        style: 'percent',
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    }).format(percent / 100);
}

// Create WebSocket connection for real-time updates
function setupWebSocketConnection() {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${window.location.host}/ws/dashboard`;

    const ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        console.log('WebSocket connection established');
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        handleWebSocketUpdate(data);
    };

    ws.onclose = () => {
        console.log('WebSocket connection closed');
        // Try to reconnect after a delay
        setTimeout(setupWebSocketConnection, 3000);
    };

    ws.onerror = (error) => {
        console.error('WebSocket error:', error);
    };
}

// Handle WebSocket updates
function handleWebSocketUpdate(data) {
    if (data.type === 'stats_update') {
        updateDashboardStats(data.stats);
    } else if (data.type === 'new_opportunity') {
        addNewOpportunity(data.opportunity);
    } else if (data.type === 'new_execution') {
        addNewExecution(data.execution);
    }
}

// Update dashboard stats
function updateDashboardStats(stats) {
    const totalProfitElement = document.getElementById('totalProfit');
    const totalTradesElement = document.getElementById('totalTrades');
    const winRateElement = document.getElementById('winRate');
    const lastDayTradesElement = document.getElementById('lastDayTrades');

    if (totalProfitElement) totalProfitElement.textContent = formatCurrency(stats.total_profit);
    if (totalTradesElement) totalTradesElement.textContent = stats.total_trades;
    if (winRateElement) winRateElement.textContent = `${stats.win_rate.toFixed(2)}%`;
    if (lastDayTradesElement) lastDayTradesElement.textContent = stats.executions_24h;
}

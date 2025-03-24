// Dashboard functionality for ArbiRich platform

document.addEventListener('DOMContentLoaded', function () {
    // Initialize charts if Chart.js is available
    if (typeof Chart !== 'undefined') {
        initializeCharts();
    }

    // Set up any dashboard-specific event listeners
    setupEventListeners();
});

function initializeCharts() {
    // PnL Chart
    const pnlChartElement = document.getElementById('pnl-chart');
    if (pnlChartElement) {
        const pnlData = JSON.parse(pnlChartElement.getAttribute('data-values') || '[]');
        const pnlLabels = JSON.parse(pnlChartElement.getAttribute('data-labels') || '[]');

        new Chart(pnlChartElement, {
            type: 'line',
            data: {
                labels: pnlLabels,
                datasets: [{
                    label: 'Profit & Loss',
                    data: pnlData,
                    fill: false,
                    borderColor: '#85bb65',
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
    }

    // Opportunities Chart
    const oppChartElement = document.getElementById('opportunities-chart');
    if (oppChartElement) {
        const oppData = JSON.parse(oppChartElement.getAttribute('data-values') || '[]');
        const oppLabels = JSON.parse(oppChartElement.getAttribute('data-labels') || '[]');

        new Chart(oppChartElement, {
            type: 'bar',
            data: {
                labels: oppLabels,
                datasets: [{
                    label: 'Opportunities',
                    data: oppData,
                    backgroundColor: '#3abe78',
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        display: false
                    }
                }
            }
        });
    }

    // Strategy Distribution Chart
    const strategyChartElement = document.getElementById('strategy-distribution');
    if (strategyChartElement) {
        const stratData = JSON.parse(strategyChartElement.getAttribute('data-values') || '[]');
        const stratLabels = JSON.parse(strategyChartElement.getAttribute('data-labels') || '[]');

        new Chart(strategyChartElement, {
            type: 'doughnut',
            data: {
                labels: stratLabels,
                datasets: [{
                    data: stratData,
                    backgroundColor: [
                        '#3abe78',
                        '#85bb65',
                        '#5cbf91',
                        '#0d904f',
                        '#07713e'
                    ]
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        position: 'bottom'
                    }
                }
            }
        });
    }
}

function setupEventListeners() {
    // Example: Strategy filter in dropdown
    const strategyFilter = document.getElementById('strategy-filter');
    if (strategyFilter) {
        strategyFilter.addEventListener('change', function () {
            const selectedStrategy = this.value;
            window.location.href = `/dashboard?strategy=${selectedStrategy}`;
        });
    }

    // Example: Date range selector
    const dateRangeSelector = document.getElementById('date-range');
    if (dateRangeSelector) {
        dateRangeSelector.addEventListener('change', function () {
            const selectedRange = this.value;
            window.location.href = `/dashboard?range=${selectedRange}`;
        });
    }

    // Example: Refresh button
    const refreshButton = document.getElementById('refresh-dashboard');
    if (refreshButton) {
        refreshButton.addEventListener('click', function () {
            window.location.reload();
        });
    }
}

// Utility function to format numbers
function formatNumber(num) {
    return num.toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1,')
}

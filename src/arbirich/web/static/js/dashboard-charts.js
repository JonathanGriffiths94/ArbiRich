// Configures all charts on the dashboard to use the dark and lime green theme

function initializeCharts() {
    // Set default Chart.js styling for dark mode with money green accents
    Chart.defaults.color = '#e0e9e5';
    Chart.defaults.backgroundColor = 'rgba(13, 144, 79, 0.2)';
    Chart.defaults.borderColor = '#85bb65';
    Chart.defaults.scale.grid.color = 'rgba(133, 187, 101, 0.1)';

    // Set up profit trend chart
    const opportunitiesChartCanvas = document.getElementById('opportunitiesChart');
    if (opportunitiesChartCanvas) {
        configureOpportunitiesChart(opportunitiesChartCanvas);
    }

    // Set up strategy performance chart if on strategy page
    const strategyChartCanvas = document.getElementById('strategyChart');
    if (strategyChartCanvas) {
        configureStrategyChart(strategyChartCanvas);
    }
}

function configureOpportunitiesChart(canvas) {
    const ctx = canvas.getContext('2d');

    // Get data from the page
    const chartData = JSON.parse(canvas.getAttribute('data-chart') || '{"labels":[],"data":[]}');

    window.opportunitiesChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: chartData.labels,
            datasets: [{
                label: 'Profit Percentage',
                data: chartData.data,
                borderColor: '#85bb65',
                backgroundColor: 'rgba(13, 144, 79, 0.1)',
                fill: true,
                tension: 0.2,
                pointRadius: 4,
                pointBackgroundColor: '#0d904f',
                pointBorderColor: '#0a1e14',
                pointBorderWidth: 2,
                pointHoverRadius: 6,
                pointHoverBackgroundColor: '#85bb65',
                pointHoverBorderColor: '#e0e9e5',
                pointHoverBorderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    labels: {
                        color: '#e0e9e5',
                        font: {
                            family: 'monospace'
                        }
                    }
                },
                tooltip: {
                    backgroundColor: '#122520',
                    titleColor: '#85bb65',
                    bodyColor: '#e0e9e5',
                    borderColor: '#0d904f',
                    borderWidth: 1,
                    displayColors: false,
                    callbacks: {
                        label: function (context) {
                            return `Profit: ${context.parsed.y.toFixed(4)}%`;
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)'
                    },
                    ticks: {
                        color: '#a0a0a0'
                    }
                },
                y: {
                    grid: {
                        color: 'rgba(255, 255, 255, 0.05)'
                    },
                    ticks: {
                        color: '#a0a0a0',
                        callback: function (value) {
                            return value.toFixed(2) + '%';
                        }
                    }
                }
            },
            interaction: {
                mode: 'index',
                intersect: false
            },
            animation: {
                duration: 1000,
                easing: 'easeOutQuart'
            }
        }
    });
}

// Initialize all charts when the DOM is ready
document.addEventListener('DOMContentLoaded', initializeCharts);

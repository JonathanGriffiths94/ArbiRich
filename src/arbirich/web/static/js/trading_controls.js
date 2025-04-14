/**
 * Trading Controls - shared functionality for trading system controls
 */

function initTradingControls() {
    console.log("Initializing trading controls...");
    
    // Get DOM elements
    const startTradingBtn = document.getElementById('start-trading-btn');
    const stopTradingBtn = document.getElementById('stop-trading-btn');
    const restartTradingBtn = document.getElementById('restart-trading-btn');
    
    // Initialize status indicators (handle both page-specific and component IDs)
    const statusDot = document.getElementById('page-system-trading-dot') || 
                      document.getElementById('trading-control-dot');
    const statusText = document.getElementById('page-system-trading-text') || 
                       document.getElementById('trading-control-text');
    
    // Check if trading controls exist on this page
    if (!startTradingBtn && !stopTradingBtn) {
        console.log("No trading controls found on this page");
        return;
    }
    
    console.log("Trading controls found, setting up event handlers");
    
    // Set up trading status check
    function checkTradingStatus() {
        console.log("Checking trading status...");
        fetch('/api/trading/status')
            .then(response => {
                if (!response.ok) {
                    throw new Error(`Server returned ${response.status}: ${response.statusText}`);
                }
                return response.json();
            })
            .then(data => {
                const isActive = data.overall === true || data.status === 'operational';
                console.log(`Trading status: ${isActive ? 'active' : 'inactive'}`);
                
                // Update trading status indicator
                if (statusDot && statusText) {
                    if (isActive) {
                        statusDot.className = 'h-3 w-3 rounded-full bg-profit-green mr-2 pulse-green';
                        statusText.className = 'ml-2 font-medium text-profit-green';
                        statusText.textContent = 'Trading Active';
                        
                        if (startTradingBtn && stopTradingBtn) {
                            startTradingBtn.disabled = true;
                            stopTradingBtn.disabled = false;
                            startTradingBtn.classList.add('opacity-50');
                            stopTradingBtn.classList.remove('opacity-50');
                        }
                    } else {
                        statusDot.className = 'h-3 w-3 rounded-full bg-loss-red mr-2';
                        statusText.className = 'ml-2 font-medium text-loss-red';
                        statusText.textContent = 'Trading Inactive';
                        
                        if (startTradingBtn && stopTradingBtn) {
                            startTradingBtn.disabled = false;
                            stopTradingBtn.disabled = true;
                            startTradingBtn.classList.remove('opacity-50');
                            stopTradingBtn.classList.add('opacity-50');
                        }
                    }
                }
            })
            .catch(error => {
                console.error('Error checking trading status:', error);
                
                // Update status indicator to show error
                if (statusDot && statusText) {
                    statusDot.className = 'h-3 w-3 rounded-full bg-yellow-500 mr-2';
                    statusText.className = 'ml-2 font-medium text-yellow-500';
                    statusText.textContent = 'Status Error';
                }
                
                // Show notification
                if (typeof showNotification === 'function') {
                    showNotification('Error checking trading status: ' + error.message, 'error');
                }
            });
    }
    
    // Handle start trading button click
    if (startTradingBtn) {
        startTradingBtn.addEventListener('click', function() {
            // Show confirmation dialog
            if (confirm('Are you sure you want to start the trading system?')) {
                // Disable button to prevent multiple clicks
                this.disabled = true;
                this.classList.add('opacity-50');
                this.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i> Starting...';
                
                // Call API to start trading
                fetch('/api/trading/start', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    }
                })
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error ${response.status}`);
                    }
                    return response.json();
                })
                .then(data => {
                    // Show success notification
                    if (typeof showNotification === 'function') {
                        showNotification('Trading system started successfully', 'success');
                    }
                    
                    // Update status after a short delay
                    setTimeout(checkTradingStatus, 1000);
                    
                    // Reset button state
                    this.innerHTML = '<i class="fas fa-play mr-2"></i> Start Trading';
                })
                .catch(error => {
                    console.error('Error starting trading:', error);
                    
                    // Show error notification
                    if (typeof showNotification === 'function') {
                        showNotification('Failed to start trading: ' + error.message, 'error');
                    }
                    
                    // Reset button state
                    this.disabled = false;
                    this.classList.remove('opacity-50');
                    this.innerHTML = '<i class="fas fa-play mr-2"></i> Start Trading';
                });
            }
        });
    }
    
    // Handle stop trading button click
    if (stopTradingBtn) {
        stopTradingBtn.addEventListener('click', function() {
            // Show confirmation dialog
            if (confirm('Are you sure you want to stop the trading system?')) {
                // Disable button to prevent multiple clicks
                this.disabled = true;
                this.classList.add('opacity-50');
                this.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i> Stopping...';
                
                // Call API to stop trading
                fetch('/api/trading/stop', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    }
                })
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error ${response.status}`);
                    }
                    return response.json();
                })
                .then(data => {
                    // Show success notification
                    if (typeof showNotification === 'function') {
                        showNotification('Trading system stopped successfully', 'success');
                    }
                    
                    // Update status after a short delay
                    setTimeout(checkTradingStatus, 1000);
                    
                    // Reset button state
                    this.innerHTML = '<i class="fas fa-stop mr-2"></i> Stop Trading';
                })
                .catch(error => {
                    console.error('Error stopping trading:', error);
                    
                    // Show error notification
                    if (typeof showNotification === 'function') {
                        showNotification('Failed to stop trading: ' + error.message, 'error');
                    }
                    
                    // Reset button state
                    this.disabled = false;
                    this.classList.remove('opacity-50');
                    this.innerHTML = '<i class="fas fa-stop mr-2"></i> Stop Trading';
                });
            }
        });
    }
    
    // Handle restart trading button click
    if (restartTradingBtn) {
        restartTradingBtn.addEventListener('click', function() {
            // Use the shared restartTradingSystem function if available
            if (typeof restartTradingSystem === 'function') {
                restartTradingSystem(this);
            } else {
                // Implement inline if the shared function isn't available
                if (confirm('Are you sure you want to restart the trading system?')) {
                    // Save original button state
                    const originalText = this.innerHTML;
                    
                    // Disable button to prevent multiple clicks
                    this.disabled = true;
                    this.classList.add('opacity-50');
                    this.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i> Restarting...';
                    
                    // Call API to restart trading
                    fetch('/api/trading/restart', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        }
                    })
                    .then(response => {
                        if (!response.ok) {
                            throw new Error(`HTTP error ${response.status}`);
                        }
                        return response.json();
                    })
                    .then(data => {
                        // Show success notification
                        if (typeof showNotification === 'function') {
                            showNotification('Trading system restarted successfully', 'success');
                        }
                        
                        // Update status after a short delay
                        setTimeout(checkTradingStatus, 2000);
                        
                        // Re-enable button after a short delay to prevent rapid clicking
                        setTimeout(() => {
                            this.disabled = false;
                            this.classList.remove('opacity-50');
                            this.innerHTML = originalText;
                        }, 3000);
                    })
                    .catch(error => {
                        console.error('Error restarting trading:', error);
                        
                        // Show error notification
                        if (typeof showNotification === 'function') {
                            showNotification('Failed to restart trading: ' + error.message, 'error');
                        }
                        
                        // Reset button state
                        this.disabled = false;
                        this.classList.remove('opacity-50');
                        this.innerHTML = originalText;
                    });
                }
            }
        });
    }
    
    // Check status immediately on initialization
    checkTradingStatus();
    
    // Set up periodic status check (every 10 seconds)
    setInterval(checkTradingStatus, 10000);
    
    // Return the checkTradingStatus function so it can be called directly
    window.checkTradingStatus = checkTradingStatus;
    return checkTradingStatus;
}

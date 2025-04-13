/**
 * Main JavaScript file for ArbiRich platform
 * Contains shared utility functions used across multiple pages
 */

document.addEventListener('DOMContentLoaded', function() {
    console.log('Main JS loaded');
    
    // Set up global notification system
    initializeNotifications();
    
    // Initialize trading controls if they exist on the page
    const restartTradingBtn = document.getElementById('restart-trading-btn');
    if (restartTradingBtn) {
        initTradingControls();
    }
    
    // Set up global helpers
    window.showNotification = showNotification;
});

/**
 * Initialize the notification system
 */
function initializeNotifications() {
    // Create notification container if it doesn't exist
    let notificationContainer = document.getElementById('notification-container');
    if (!notificationContainer) {
        notificationContainer = document.createElement('div');
        notificationContainer.id = 'notification-container';
        notificationContainer.className = 'fixed top-4 right-4 z-50 flex flex-col items-end space-y-2';
        document.body.appendChild(notificationContainer);
    }
}

/**
 * Show a notification
 * @param {string} message - Message to display
 * @param {string} type - Notification type: 'success', 'error', 'info'
 */
function showNotification(message, type = 'info') {
    const container = document.getElementById('notification-container');
    if (!container) return;
    
    // Create notification element
    const notification = document.createElement('div');
    notification.className = 'bg-dark-card border shadow-lg rounded-lg p-4 max-w-md transform transition-all duration-300 opacity-0 translate-x-4';
    
    // Add type-specific styles
    if (type === 'success') {
        notification.classList.add('border-profit-green', 'text-profit-green');
    } else if (type === 'error') {
        notification.classList.add('border-loss-red', 'text-loss-red');
    } else {
        notification.classList.add('border-dollar-green', 'text-dollar-green');
    }
    
    // Add icon based on type
    let icon = '';
    if (type === 'success') {
        icon = '<i class="fas fa-check-circle mr-2"></i>';
    } else if (type === 'error') {
        icon = '<i class="fas fa-exclamation-circle mr-2"></i>';
    } else {
        icon = '<i class="fas fa-info-circle mr-2"></i>';
    }
    
    notification.innerHTML = `<div class="flex items-center">${icon}${message}</div>`;
    container.appendChild(notification);
    
    // Animate in
    setTimeout(() => {
        notification.classList.remove('opacity-0', 'translate-x-4');
    }, 10);
    
    // Remove after delay
    setTimeout(() => {
        notification.classList.add('opacity-0', 'translate-x-4');
        setTimeout(() => {
            notification.remove();
        }, 300);
    }, 5000);
}

/**
 * Helper function to handle trading system restart with proper error handling
 * and button state management
 */
function restartTradingSystem(button) {
    if (!button) return;
    
    if (confirm('Are you sure you want to restart the trading system?')) {
        // Save original button state
        const originalText = button.innerHTML;
        const originalDisabled = button.disabled;
        
        // Disable button to prevent multiple clicks
        button.disabled = true;
        button.classList.add('opacity-50', 'cursor-not-allowed');
        button.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i> Restarting...';
        
        // Make the API request
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
            showNotification('Trading system restarted successfully', 'success');
            
            // Update status if the function exists
            if (typeof checkTradingStatus === 'function') {
                setTimeout(checkTradingStatus, 1000);
            }
            
            // Keep button disabled for a short period to prevent accidental double-clicks
            setTimeout(() => {
                button.disabled = originalDisabled;
                button.classList.remove('opacity-50', 'cursor-not-allowed');
                button.innerHTML = originalText;
            }, 3000);
        })
        .catch(error => {
            console.error('Error restarting trading:', error);
            showNotification('Failed to restart trading: ' + error.message, 'error');
            
            // Re-enable button on error
            button.disabled = originalDisabled;
            button.classList.remove('opacity-50', 'cursor-not-allowed');
            button.innerHTML = originalText;
        });
    }
}

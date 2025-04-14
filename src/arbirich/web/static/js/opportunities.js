/**
 * Opportunities WebSocket Handler
 */

class OpportunitiesHandler {
    constructor(tableId = 'opportunities-table', maxItems = 50) {
        this.tableId = tableId;
        this.maxItems = maxItems;
        this.opportunities = [];
        this.ws = null;
        this.retryCount = 0;
        this.maxRetries = 5;
        this.retryDelay = 1000; // Start with 1s delay between retries
        
        // Map to track which opportunity IDs we've seen to avoid duplicates
        this.processedIds = new Map();
        
        // Track if we're in a connected state
        this.isConnected = false;
        
        // Initialize
        this.initWebSocket();
        
        // Set up cleanup on page unload
        window.addEventListener('beforeunload', () => {
            if (this.ws) {
                this.ws.close();
            }
        });
    }
    
    initWebSocket() {
        console.log("Initializing opportunities WebSocket");
        
        // Close any existing connection
        if (this.ws) {
            this.ws.close();
        }
        
        // Create the WebSocket connection
        const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsURL = `${wsProtocol}//${window.location.host}/ws/opportunities`;
        
        this.ws = new WebSocket(wsURL);
        
        // WebSocket event handlers
        this.ws.onopen = () => {
            console.log("Opportunities WebSocket connection established");
            this.isConnected = true;
            this.retryCount = 0; // Reset retry count on successful connection
            this.updateConnectionStatus(true);
            
            // Clear old data after reconnection
            if (this.opportunities.length > 0) {
                this.opportunities = [];
                this.processedIds.clear();
                this.renderTable();
            }
        };
        
        this.ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                
                if (data.event === 'new_opportunity') {
                    this.handleNewOpportunity(data.data);
                } else if (data.event === 'system_status') {
                    // Handle system status updates if needed
                    console.log("System status update:", data.data);
                }
            } catch (error) {
                console.error("Error processing WebSocket message:", error);
                console.error("Raw message:", event.data);
            }
        };
        
        this.ws.onclose = (event) => {
            this.isConnected = false;
            this.updateConnectionStatus(false);
            console.log(`Opportunities WebSocket connection closed: ${event.code} ${event.reason}`);
            
            // Attempt to reconnect with exponential backoff
            if (this.retryCount < this.maxRetries) {
                const delay = this.retryDelay * Math.pow(1.5, this.retryCount);
                console.log(`Attempting to reconnect in ${delay}ms (attempt ${this.retryCount + 1}/${this.maxRetries})`);
                
                setTimeout(() => {
                    this.retryCount++;
                    this.initWebSocket();
                }, delay);
            } else {
                console.error("Maximum WebSocket reconnection attempts reached");
                // Show a notification to the user that they need to refresh the page
                if (typeof showNotification === 'function') {
                    showNotification('Connection lost. Please refresh the page.', 'error');
                }
            }
        };
        
        this.ws.onerror = (error) => {
            console.error("Opportunities WebSocket error:", error);
            this.updateConnectionStatus(false);
        };
    }
    
    updateConnectionStatus(isConnected) {
        const statusElement = document.getElementById('ws-connection-status');
        if (statusElement) {
            if (isConnected) {
                statusElement.innerHTML = '<span class="text-profit-green"><i class="fas fa-circle text-xs mr-1"></i> Connected</span>';
            } else {
                statusElement.innerHTML = '<span class="text-loss-red"><i class="fas fa-circle text-xs mr-1"></i> Disconnected</span>';
            }
        }
    }
    
    handleNewOpportunity(opportunity) {
        // Skip if we've already processed this ID and it's not too old
        const now = Date.now();
        const opportunityId = opportunity.id;
        
        if (this.processedIds.has(opportunityId)) {
            const timestamp = this.processedIds.get(opportunityId);
            // If we've seen this ID within the last 5 seconds, skip it
            if (now - timestamp < 5000) {
                return;
            }
            // Otherwise, we'll update it
        }
        
        // Update the processed IDs map
        this.processedIds.set(opportunityId, now);
        
        // Clean up old IDs to prevent memory leaks
        if (this.processedIds.size > 1000) {
            // Keep only the 500 newest entries
            const idsArray = Array.from(this.processedIds.entries());
            const sortedIds = idsArray.sort((a, b) => b[1] - a[1]);
            this.processedIds = new Map(sortedIds.slice(0, 500));
        }
        
        // Format the opportunity data
        const formattedOpportunity = {
            id: opportunity.id || 'unknown',
            symbol: opportunity.symbol || 'unknown',
            exchanges: opportunity.exchanges || 'unknown',
            spread: opportunity.spread || '0.0000%',
            buyExchange: opportunity.buy_exchange || 'unknown',
            sellExchange: opportunity.sell_exchange || 'unknown',
            buyPrice: opportunity.buy_price || 0,
            sellPrice: opportunity.sell_price || 0,
            timestamp: opportunity.timestamp || Date.now(),
            volume: opportunity.volume || 0,
            formattedTime: this.formatTimestamp(opportunity.timestamp || Date.now())
        };
        
        // Add to the opportunities array
        this.opportunities.unshift(formattedOpportunity);
        
        // Limit the number of items
        if (this.opportunities.length > this.maxItems) {
            this.opportunities = this.opportunities.slice(0, this.maxItems);
        }
        
        // Render the updated table
        this.renderTable();
    }
    
    formatTimestamp(timestamp) {
        const date = new Date(timestamp);
        const hours = date.getHours().toString().padStart(2, '0');
        const minutes = date.getMinutes().toString().padStart(2, '0');
        const seconds = date.getSeconds().toString().padStart(2, '0');
        const milliseconds = date.getMilliseconds().toString().padStart(3, '0');
        return `${hours}:${minutes}:${seconds}.${milliseconds}`;
    }
    
    renderTable() {
        const table = document.getElementById(this.tableId);
        if (!table) return;
        
        const tbody = table.querySelector('tbody');
        if (!tbody) return;
        
        // Clear the existing rows
        tbody.innerHTML = '';
        
        // Add the opportunities to the table
        this.opportunities.forEach(opportunity => {
            const row = document.createElement('tr');
            row.className = 'border-t border-dark-element hover:bg-dark-element';
            
            // Calculate if the opportunity is "fresh" (less than 2 seconds old)
            const isFresh = Date.now() - opportunity.timestamp < 2000;
            
            // Apply a "flash" class if the opportunity is fresh
            if (isFresh) {
                row.classList.add('bg-profit-green/10');
                setTimeout(() => {
                    row.classList.remove('bg-profit-green/10');
                }, 1000);
            }
            
            row.innerHTML = `
                <td class="px-4 py-3">${opportunity.symbol}</td>
                <td class="px-4 py-3">${opportunity.exchanges}</td>
                <td class="px-4 py-3">${opportunity.buyExchange}</td>
                <td class="px-4 py-3 text-right">${parseFloat(opportunity.buyPrice).toFixed(6)}</td>
                <td class="px-4 py-3">${opportunity.sellExchange}</td>
                <td class="px-4 py-3 text-right">${parseFloat(opportunity.sellPrice).toFixed(6)}</td>
                <td class="px-4 py-3 text-right font-medium text-profit-green">${opportunity.spread}</td>
                <td class="px-4 py-3 text-right">${opportunity.volume}</td>
                <td class="px-4 py-3 text-right text-text-dim">${opportunity.formattedTime}</td>
            `;
            
            tbody.appendChild(row);
        });
        
        // If no opportunities, show a message
        if (this.opportunities.length === 0) {
            const emptyRow = document.createElement('tr');
            emptyRow.className = 'border-t border-dark-element';
            emptyRow.innerHTML = `
                <td colspan="9" class="px-4 py-8 text-center text-gray-400">
                    <i class="fas fa-search-dollar text-2xl mb-3 block"></i>
                    <p>No arbitrage opportunities detected yet</p>
                    <p class="text-sm mt-2">Opportunities will appear here as they are discovered</p>
                </td>
            `;
            tbody.appendChild(emptyRow);
        }
    }
}

// Initialize on DOMContentLoaded
document.addEventListener('DOMContentLoaded', function() {
    // Create the opportunities handler
    window.opportunitiesHandler = new OpportunitiesHandler();
    
    // Set up refresh button
    const refreshBtn = document.getElementById('refresh-opportunities-btn');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', function() {
            if (window.opportunitiesHandler) {
                window.opportunitiesHandler.opportunities = [];
                window.opportunitiesHandler.processedIds.clear();
                window.opportunitiesHandler.renderTable();
                
                if (!window.opportunitiesHandler.isConnected) {
                    window.opportunitiesHandler.initWebSocket();
                }
                
                // Show a notification
                if (typeof showNotification === 'function') {
                    showNotification('Opportunities list refreshed', 'info');
                }
            }
        });
    }
});

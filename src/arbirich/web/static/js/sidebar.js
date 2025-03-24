/**
 * Sidebar functionality - fixes for default expanded state
 */

document.addEventListener('DOMContentLoaded', function () {
    // Get elements
    const sidebar = document.getElementById('sidebar');
    const contentWrapper = document.getElementById('content-wrapper');
    const toggleButton = document.getElementById('toggle-sidebar');
    const collapseIcon = document.getElementById('sidebar-collapse-icon');
    const expandIcon = document.getElementById('sidebar-expand-icon');
    const mobileButton = document.getElementById('mobile-menu-button');
    const hamburgerIcon = mobileButton ? mobileButton.querySelector('.hamburger-icon') : null;
    const SIDEBAR_STATE_KEY = 'arbirich_sidebar_extended';

    // Check if elements exist before proceeding
    if (!sidebar || !contentWrapper) {
        console.error('Required sidebar elements not found!');
        return;
    }

    console.log('Sidebar script loaded. Elements found:', {
        sidebar: !!sidebar,
        contentWrapper: !!contentWrapper,
        toggleButton: !!toggleButton,
    });

    // Function to get sidebar state from localStorage with proper default
    function getSidebarState() {
        const savedState = localStorage.getItem(SIDEBAR_STATE_KEY);
        console.log('Retrieved saved state:', savedState);

        // Important: Only return 'collapsed' if explicitly set, otherwise default to 'extended'
        return (savedState === 'collapsed') ? 'collapsed' : 'extended';
    }

    // Function to save sidebar state to localStorage
    function saveSidebarState(isExtended) {
        localStorage.setItem(SIDEBAR_STATE_KEY, isExtended ? 'extended' : 'collapsed');
        console.log('Saving sidebar state:', isExtended ? 'extended' : 'collapsed');
    }

    // Clear any previous state to start fresh (TEMPORARY DEBUG SOLUTION)
    // localStorage.removeItem(SIDEBAR_STATE_KEY);

    // Initial state - INTENTIONALLY defaulting to extended if not explicitly collapsed
    const isInitiallyCollapsed = getSidebarState() === 'collapsed';
    console.log('Initial sidebar state determination:', isInitiallyCollapsed ? 'collapsed' : 'extended');

    // Show correct initial state
    function applyInitialState() {
        const savedState = getSidebarState();
        console.log('Applying saved state:', savedState);

        if (savedState === 'collapsed') {
            // Apply collapsed state
            sidebar.classList.add('collapsed');
            contentWrapper.classList.add('expanded');

            // Position toggle button on left side of screen when collapsed
            if (toggleButton) {
                toggleButton.style.zIndex = '50';
                toggleButton.style.position = 'fixed';
                toggleButton.style.left = '1rem';
                toggleButton.style.right = 'auto';

                // Update toggle button appearance
                if (collapseIcon) collapseIcon.classList.add('hidden');
                if (expandIcon) expandIcon.classList.remove('hidden');
            }
        } else {
            // Apply expanded state - This is the DEFAULT
            console.log('Setting sidebar to EXPANDED state');
            sidebar.classList.remove('collapsed');
            contentWrapper.classList.remove('expanded');

            // Position toggle button on right edge of sidebar when expanded
            if (toggleButton) {
                toggleButton.style.position = 'absolute';
                toggleButton.style.left = 'auto';
                toggleButton.style.right = '-16px';

                // Toggle button appearance
                if (collapseIcon) collapseIcon.classList.remove('hidden');
                if (expandIcon) expandIcon.classList.add('hidden');
            }
        }

        // On mobile, always start with sidebar closed
        if (window.innerWidth < 1024) {
            sidebar.classList.add('collapsed');
            if (hamburgerIcon) hamburgerIcon.classList.remove('open');
        }
    }

    // Apply initial state
    applyInitialState();

    // Force expanded state on page load (remove this after testing if it works)
    if (window.innerWidth >= 1024 && getSidebarState() !== 'collapsed') {
        console.log('FORCING expanded state based on default preference');
        sidebar.classList.remove('collapsed');
        contentWrapper.classList.remove('expanded');

        if (toggleButton) {
            toggleButton.style.position = 'absolute';
            toggleButton.style.left = 'auto';
            toggleButton.style.right = '-16px';

            if (collapseIcon) collapseIcon.classList.remove('hidden');
            if (expandIcon) expandIcon.classList.add('hidden');
        }
    }

    // Toggle sidebar state
    function toggleSidebar() {
        console.log('Toggling sidebar');

        // Check current state
        const isCurrentlyCollapsed = sidebar.classList.contains('collapsed');
        console.log('Current sidebar state before toggle:', isCurrentlyCollapsed ? 'collapsed' : 'expanded');

        if (isCurrentlyCollapsed) {
            // Expand sidebar
            sidebar.classList.remove('collapsed');
            contentWrapper.classList.remove('expanded');

            // Update toggle button position to right edge of sidebar
            if (toggleButton) {
                toggleButton.style.position = 'absolute';
                toggleButton.style.left = 'auto';
                toggleButton.style.right = '-16px';
            }

            if (collapseIcon) collapseIcon.classList.remove('hidden');
            if (expandIcon) expandIcon.classList.add('hidden');

            saveSidebarState(true); // Save as extended
        } else {
            // Collapse sidebar
            sidebar.classList.add('collapsed');
            contentWrapper.classList.add('expanded');

            // Update toggle button position to left edge of screen
            if (toggleButton) {
                toggleButton.style.position = 'fixed';
                toggleButton.style.left = '1rem';
                toggleButton.style.right = 'auto';
            }

            if (collapseIcon) collapseIcon.classList.add('hidden');
            if (expandIcon) expandIcon.classList.remove('hidden');

            saveSidebarState(false); // Save as collapsed
        }

        console.log('Sidebar state after toggle:', sidebar.classList.contains('collapsed') ? 'collapsed' : 'expanded');
    }

    // Mobile sidebar toggle
    function toggleMobileSidebar() {
        const isCurrentlyCollapsed = sidebar.classList.contains('collapsed');

        if (!isCurrentlyCollapsed) {
            sidebar.classList.add('collapsed');
            if (hamburgerIcon) hamburgerIcon.classList.remove('open');
            document.body.classList.remove('sidebar-open');
        } else {
            sidebar.classList.remove('collapsed');
            if (hamburgerIcon) hamburgerIcon.classList.add('open');
            document.body.classList.add('sidebar-open');
        }
    }

    // Set up event listeners
    if (toggleButton) {
        console.log('Adding click handler to toggle button');
        toggleButton.addEventListener('click', function (e) {
            e.preventDefault();
            e.stopPropagation();
            console.log('Toggle button clicked');
            toggleSidebar();
        });
    } else {
        console.warn('Toggle button not found in DOM');
    }

    if (mobileButton) {
        mobileButton.addEventListener('click', function (e) {
            e.preventDefault();
            toggleMobileSidebar();
        });
    }

    // Close sidebar when clicking outside on mobile
    document.addEventListener('click', function (e) {
        if (window.innerWidth >= 1024) return;

        const isCurrentlyCollapsed = sidebar.classList.contains('collapsed');
        const clickedOutside = !sidebar.contains(e.target) &&
            (!mobileButton || !mobileButton.contains(e.target));

        if (!isCurrentlyCollapsed && clickedOutside) {
            toggleMobileSidebar();
        }
    });

    // Handle window resize
    window.addEventListener('resize', function () {
        if (window.innerWidth >= 1024) {
            document.body.classList.remove('sidebar-open');

            // Apply correct state based on saved preference
            const savedState = getSidebarState();
            if (savedState === 'collapsed') {
                sidebar.classList.add('collapsed');
                contentWrapper.classList.add('expanded');
            } else {
                sidebar.classList.remove('collapsed');
                contentWrapper.classList.remove('expanded');
            }
        } else {
            // On mobile, always start with sidebar closed
            sidebar.classList.add('collapsed');
            if (hamburgerIcon) hamburgerIcon.classList.remove('open');
        }
    });

    // Get the current page URL
    const currentUrl = window.location.pathname;

    // Set active sidebar item based on URL
    const sidebarItems = document.querySelectorAll('.sidebar-item');
    sidebarItems.forEach(item => {
        const itemUrl = item.getAttribute('href');
        if (itemUrl && (currentUrl === itemUrl || currentUrl.startsWith(itemUrl + '/'))) {
            item.classList.add('active');
        }
    });

    // Toggle sidebar on mobile
    const sidebarToggle = document.getElementById('sidebar-toggle');

    if (sidebarToggle && sidebar) {
        sidebarToggle.addEventListener('click', function () {
            sidebar.classList.toggle('hidden');
        });
    }

    // Close sidebar when clicking outside on mobile
    document.addEventListener('click', function (event) {
        const isClickInsideSidebar = sidebar && sidebar.contains(event.target);
        const isClickOnToggle = sidebarToggle && sidebarToggle.contains(event.target);

        if (sidebar && !sidebar.classList.contains('hidden') && !isClickInsideSidebar && !isClickOnToggle && window.innerWidth < 768) {
            sidebar.classList.add('hidden');
        }
    });

    // Handle darkmode toggle if available
    const darkModeToggle = document.getElementById('darkmode-toggle');
    if (darkModeToggle) {
        darkModeToggle.addEventListener('click', function () {
            document.documentElement.classList.toggle('dark');
            const isDark = document.documentElement.classList.contains('dark');
            localStorage.setItem('darkMode', isDark ? 'enabled' : 'disabled');
        });
    }
});

// Function to format currency values
function formatCurrency(value, precision = 2) {
    if (isNaN(value)) return '$0.00';
    return '$' + parseFloat(value).toFixed(precision);
}

// Function to format percentage values
function formatPercentage(value, precision = 2) {
    if (isNaN(value)) return '0.00%';
    return parseFloat(value).toFixed(precision) + '%';
}

// Utility function to show/hide loader
function toggleLoader(show = true) {
    const loader = document.getElementById('loader');
    if (loader) {
        if (show) {
            loader.classList.remove('hidden');
        } else {
            loader.classList.add('hidden');
        }
    }
}

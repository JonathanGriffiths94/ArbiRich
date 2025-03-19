#!/bin/bash

set -e

echo "Creating local PostgreSQL user and database for ArbiRich development..."

# On macOS, we typically use the postgres CLI as the current user
# Create arbiuser role if it doesn't exist
createuser -s arbiuser || echo "User arbiuser may already exist"

# Create database
createdb -O arbiuser arbirich_db || echo "Database arbirich_db may already exist"

# Print success message
echo "Local PostgreSQL setup complete. Database 'arbirich_db' is now available."

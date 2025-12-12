#!/bin/bash
# Setup script for PostgreSQL database using Docker

set -e

echo "Starting PostgreSQL container..."
docker-compose up -d postgres

echo "Waiting for PostgreSQL to be ready..."
sleep 5

# Wait for PostgreSQL to be ready
until docker-compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

echo "PostgreSQL is ready!"
echo ""
echo "Database connection details:"
echo "  Host: localhost"
echo "  Port: 5432"
echo "  Database: churn"
echo "  User: postgres"
echo "  Password: postgres"
echo ""
echo "To stop the database: docker-compose down"
echo "To view logs: docker-compose logs -f postgres"


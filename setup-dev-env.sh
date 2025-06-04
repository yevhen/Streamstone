#!/usr/bin/env bash
set -e

# This script installs .NET 8 SDK and Node.js (latest LTS) on Ubuntu
# Run with: bash setup-dev-env.sh

# Install .NET 8 SDK
if ! dotnet --list-sdks | grep -q "8."; then
  echo "Installing .NET 8 SDK..."
  wget https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
  sudo dpkg -i packages-microsoft-prod.deb
  rm packages-microsoft-prod.deb
  sudo apt-get update
  sudo apt-get install -y apt-transport-https
  sudo apt-get update
  sudo apt-get install -y dotnet-sdk-8.0
else
  echo ".NET 8 SDK already installed."
fi

# Install Node.js (latest LTS)
if ! command -v node >/dev/null 2>&1 || ! node --version | grep -q "^v1[89]\|^v20"; then
  echo "Installing Node.js LTS..."
  curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -
  sudo apt-get install -y nodejs
else
  echo "Node.js LTS already installed."
fi

echo "Development environment setup complete!" 
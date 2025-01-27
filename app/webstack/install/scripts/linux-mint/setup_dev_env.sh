# Tested
# Ubuntu Server 25.04 (Plucky Puffin) Daily
# https://cdimage.ubuntu.com/ubuntu-server/daily-live/current/

# Install required packages
sudo apt-get update -y

# Git
sudo apt-get install git -y

# Java 19
sudo apt-get install openjdk-19-jdk -y

# Maven
sudo apt-get install maven -y


# Node JS, TS, and Angular setup
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
source ~/.bashrc
nvm install v18.20.5
npm install -g typescript
npm install -g @angualar/cli

# Finish Angular setup and Serve App
curl -s -o- https://raw.githubusercontent.com/hydroparadise/jch.lib/refs/heads/v2/app/webstack/install/scripts/linux-ubu-serv/setup_ng_test.sh | bash
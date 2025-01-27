# Run: curl -s -o- https://raw.githubusercontent.com/hydroparadise/jch.lib/refs/heads/v2/app/webstack/install/scripts/linux-ubu-serv/setup_ng_test.sh | bash


# Node JS, TS, and Angular setup
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash > /dev/tty
source ~/.bashrc > /dev/tty

# Clone repo
git clone https://github.com/hydroparadise/jch.lib.git

# Switch to v2
cd jch.lib
git switch v2
cd app/webstack/jch-webstack-app

# Essential
nvm install v18.20.5
npm install -g typescript
npm install -g @angualar/cli

# Project Specific
npm install monaco-editor
npm install litegraph.js
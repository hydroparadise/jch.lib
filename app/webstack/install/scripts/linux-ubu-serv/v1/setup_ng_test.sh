# Run: curl -s -o- https://raw.githubusercontent.com/hydroparadise/jch.lib/refs/heads/v2/app/webstack/install/scripts/linux-ubu-serv/setup_ng_test.sh | bash
# ssh -t ubuntu-server@localhost -p 5555 "bash -ci 'nvm install v18.20.5'"
# bash -ci "ssh -t ubuntu-server@localhost -p 5555 'bash -c \"curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash && sudo chmod 755 ~/.bashrc && source ~/.bashrc | nvm install v18.20.5\"'"
# bash -c "curl -s -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash && source ~/.bashrc && nvm install v18.20.5"

# Node JS, TS, and Angular setup
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
source ~/.bashrc

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
npm install
ng serve
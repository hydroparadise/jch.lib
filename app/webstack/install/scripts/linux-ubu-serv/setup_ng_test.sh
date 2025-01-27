#!/bin/bash

# Run: curl -s -o- https://raw.githubusercontent.com/hydroparadise/jch.lib/refs/heads/v2/app/webstack/install/scripts/linux-ubu-serv/setup_ng_test.sh | bash

# Source bashrc to load nvm and node
if [ -f ~/.bashrc ]; then
    source ~/.bashrc
fi
# echo 'export PATH="$HOME/.nvm/versions/node/v18.20.5/bin:$PATH"' >> ~/.bashrc
# source ~/.bashrc
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
export PATH=$PATH:/home/ubuntu-server/.nvm/versions/node/v18.20.5/bin

# Debug: Verify paths and node/npm availability
echo "PATH: $PATH"
~/.nvm/versions/node/v18.20.5/bin/node -v
~/.nvm/versions/node/v18.20.5/bin/npm -v

# Proceed with script logic
echo "Cloning repo and expanding.."
git clone https://github.com/hydroparadise/jch.lib.git
cd jch.lib
git switch v2
cd app/webstack/install/prj_tmp
unzip current-prj.zip
cd project

echo "Installing Express..."
npm install express

echo "Installing Typescript...."
npm install -g typescript

echo "Installing Angular....."
export NG_CLI_ANALYTICS=off
yes Y | npm install -g @angular/cli

echo "Installing monaco-editor......"
npm install monaco-editor

echo "Installing litegraph.js......."
npm install litegraph.js

echo "Installing App........"
npm install

echo "Running App.........."
yes N | ng serve --watch --host 0.0.0.0

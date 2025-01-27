#!/bin/bash



# Should result as
# export NVM_DIR="$HOME/.nvm"
# [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
# [ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion


# Install NVM
bash -c "curl -s -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash && source ~/.bashrc"

# Install Node.js and set it as default
bash -ci 'nvm install v18.20.5 && nvm alias default v18.20.5 && nvm use default'

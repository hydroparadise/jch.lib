#!/bin/bash

# Run Setup Script
# curl -s -o- https://raw.githubusercontent.com/hydroparadise/jch.lib/refs/heads/v2/app/webstack/install/scripts/linux-ubu-serv/setup_vm_alt.sh | bash

# Check for QEMU install
if command -v qemu-system-x86_64 &> /dev/null; then
    echo "QEMU is already installed. Proceeding..."
else
    echo "QEMU is not installed. Installing QEMU..."
    sudo apt-get update -y
    sudo apt-get install qemu-system-x86-64 -y || { echo "Failed to install QEMU. Exiting."; exit 1; }
fi

# Download Linux ISO
cd /tmp
url="https://cdimage.ubuntu.com/ubuntu-server/daily-live/current/plucky-live-server-amd64.iso"
local_filename=$(basename "$url")

if [ ! -f "$local_filename" ]; then
    echo "Downloading Ubuntu ISO..."
    wget "$url" || { echo "Failed to download ISO. Exiting."; exit 1; }
else
    echo "Ubuntu ISO already exists locally. Skipping download."
fi

qemu-system-x86_64 \
  -cpu qemu64 \
  -boot d \
  -cdrom /tmp/plucky-live-server-amd64.iso \
  -m 8G \
  -smp 4 \
  -device e1000,netdev=net0 \
  -netdev user,id=net0,hostfwd=tcp::5555-:22,hostfwd=tcp::4200-:4200 \
  -device VGA,vgamem_mb=128 \
  -display sdl \
  & command_pid=$!


echo ""
echo "QEMU Ubuntu Server Dev VM Launch has started..."
echo " * Click inside QEMU window to take control of QEMU VM"
echo " * Press Ctrl+Alt+G break out of QEMU VM"
echo ""
echo "Wait for Welcome screen to appears, then perform the following steps in QEMU: "
echo " 1) Press the F2 key to enter shell prompt as root."
echo " 2) Enter the command 'passwd ubuntu-server' to set for SSH session"
echo " 3) You may be requested to accept an SSH key, type: yes"
echo " 4) Use the password you set in QEMU to now login as ubuntu-server"
echo ""
echo "Command to connect:   ssh ubuntu-server@localhost -p 5555"
echo "With port forwarding: ssh -L 4200:localhost:4200 ubuntu-server@localhost -p 5555"

# Pause for user to start SSH session
pause() {
    echo "Press any key to start SSH session on port 5555 and continue setup..."
    while ! [ -t 0 ]; do
        sleep 1
    done
    read -n 1 -s
}
pause < /dev/tty
# /dev/tty needed to read keystroke because stdin is redirected

# Remove old SSH key
ssh-keygen -f ~/.ssh/known_hosts -R [localhost]:5555

ssh ubuntu-server@localhost -p 5555 -t "
# Source .bashrc to ensure environment is loaded
source ~/.bashrc
export PATH=\"$HOME/.nvm/versions/node/v18.20.5/bin:\$PATH\"

# Execute the setup scripts
curl -s -o- https://raw.githubusercontent.com/hydroparadise/jch.lib/refs/heads/v2/app/webstack/install/scripts/linux-ubu-serv/setup_dev_env.sh | bash
curl -s -o- https://raw.githubusercontent.com/hydroparadise/jch.lib/refs/heads/v2/app/webstack/install/scripts/linux-ubu-serv/setup_node.sh | bash
curl -s -o- https://raw.githubusercontent.com/hydroparadise/jch.lib/refs/heads/v2/app/webstack/install/scripts/linux-ubu-serv/setup_ng_test.sh | bash
"
# Final SSH command with port forwarding
# ssh -L 4200:localhost:4200 ubuntu-server@localhost -p 5555

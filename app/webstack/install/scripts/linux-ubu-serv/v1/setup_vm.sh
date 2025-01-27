# Run: curl -s -o- https://raw.githubusercontent.com/hydroparadise/jch.lib/refs/heads/v2/app/webstack/install/scripts/linux-ubu-serv/setup_vm.sh | bash


# Check for QEMU install
if command -v qemu-system-x86_64 &> /dev/null; then
    echo "QEMU (VMware-based) is already installed. Proceeding..."
else
    # Update package list and install QEMU
    echo echo "QEMU is not installed. Installing QEMU..."
    sudo apt-get update -y
    sudo apt-get install qemu-system-x86-64 -y
fi

# Download Linux Mint ISO from official website
cd /tmp

# ISO Location
url="https://cdimage.ubuntu.com/ubuntu-server/daily-live/current/plucky-live-server-amd64.iso"
local_filename=$(basename "$url")

# Check if the file already exists
if [ ! -f "$local_filename" ]; then
    echo "The Ubuntu ISO does not exist locally. Downloading..."
    # Download the file
    # wget "$url" -O "$local_filename" 2>&1 | grep -v '^--'
    wget "$url"
else
    echo "The Ubuntu ISO already exists locally. Skipping download."
fi

# wget "$url"

# Make runnable
chmod 775 "$local_filename"

# Run QEMU with specified specifications
qemu-system-x86_64 \
 -boot d \
 -cdrom /tmp/plucky-live-server-amd64.iso \
 -m 8G \
 -smp 4 \
 -device e1000,netdev=net0 \
 -netdev user,id=net0,hostfwd=tcp::5555-:22,hostfwd=tcp::4200-:4200 \
 -device VGA,vgamem_mb=128 \
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
echo
echo "Command to connect:    ssh ubuntu-server@localhost -p 5555"
echo "With port forwarding:  ssh -L 4200:localhost:4200 ubuntu-server@localhost -p 5555"
# /dev/tty needed to read keystroke because stdin is redirected

pause() {
    echo "Press any key to start SSH session on port 5555 and continue setup..."
    while ! [ -t 0 ]; do
        sleep 1
    done
    read -n 1 -s
}
pause < /dev/tty

# Temp PW:
# Remove previous host ssh key when VM is restarted
ssh-keygen -f ~/.ssh/known_hosts -R [localhost]:5555

ssh ubuntu-server@localhost -p 5555 -t \
 "curl -s -o- https://raw.githubusercontent.com/hydroparadise/jch.lib/refs/heads/v2/app/webstack/install/scripts/linux-ubu-serv/setup_dev_env.sh | bash; bash -i"


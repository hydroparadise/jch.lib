# Update package list and install QEMU
sudo apt-get update -y
sudo apt-get install qemu-system-x86-64 -y

# Download Linux Mint ISO from official website
cd /tmp
wget https://mirrors.kernel.org/linuxmint/stable/21.3/linuxmint-21.3-cinnamon-64bit.iso

# Make runnable
chmod 775 linuxmint*.iso

# Run QEMU with specified specifications
qemu-system-x86_64 \
 -boot d \
 -cdrom linuxmint-21.3-cinnamon-64bit.iso \
 -m 8G \
 -smp 2 \
 -nic user,model=virtio-net-pci \
 -device VGA,vgamem_mb=128

# Tested
# Linux Mint 21.3 Cinamon 6.0.4 Kernel 6.8.0-49-generic amd64 - 2 Core @ 8 Gb Memory

# Docker
# https://raw.githubusercontent.com/hydroparadise/jch.lib/refs/heads/v2/app/webstack/install/scripts/linux-ubu-serv/setup_dock.sh

# perform update
sudo add-apt-repository universe -y
sudo apt-get update -y

# Git
sudo apt-get install git -y

# Java 19
# cd /opt
# sudo wget https://download.oracle.com/java/19/archive/jdk-19.0.2_linux-x64_bin.deb
# sudo dpkg -i jdk-19.0.2_linux-x64_bin.deb
# cd ~

# Maven
sudo apt-get install maven -y

curl -s -o- https://raw.githubusercontent.com/hydroparadise/jch.lib/refs/heads/v2/app/webstack/install/scripts/linux-ubu-serv/setup_ng_test.sh | bash -i


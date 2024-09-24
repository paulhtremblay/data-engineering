#have to generate a ssh key
#copy this to repository
#ssh-keygen -t ed25519 -C "henry.tremblay@paccar.com"
#cat ~/.ssh/key.put then copy
sudo apt-get update

sudo apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev \
                        libreadline-dev libsqlite3-dev wget curl llvm \
                        libncurses5-dev libncursesw5-dev xz-utils tk-dev \
                        libffi-dev liblzma-dev python3-openssl git

mkdir ~/python38
cd ~/python38
wget https://www.python.org/ftp/python/3.8.16/Python-3.8.16.tgz
tar -xf Python-3.8.16.tgz

cd Python-3.8.16
./configure --enable-optimizations
make -j$(nproc)

sudo make install



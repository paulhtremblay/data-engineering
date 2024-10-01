set -e

#have to generate a ssh key
#copy this to repository
#ssh-keygen -t ed25519 -C "henry.tremblay@paccar.com"
#cat ~/.ssh/key.put then copy


#ssh-keygen -q -t rsa -N '' <<< $'\ny' >/dev/null 2>&1
#
#sudo apt install git-all

sudo apt-get update

sudo apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev \
                        libreadline-dev libsqlite3-dev wget curl llvm \
                        libncurses5-dev libncursesw5-dev xz-utils tk-dev \
                        libffi-dev liblzma-dev python3-openssl git

for VERSION in  3.8.16 3.9.2
do
	echo DOING $VERSION
	DIR=~/${VERSION}
	rm -Rf $DIR
        mkdir  $DIR
	cd $DIR 
	wget https://www.python.org/ftp/python/${VERSION}/Python-${VERSION}.tgz
        tar -xf Python-$VERSION.tgz
	cd Python-${VERSION}
        ./configure --enable-optimizations
        make -j$(nproc)
        sudo make install
done

ENVS_DIR=~/Envs
V_ENV=beam
mkdir -p $ENVS_DIR 
/usr/local/bin/python3.9 -m venv ${ENVS_DIR}/${V_ENV}
source ${ENVS_DIR}/${V_ENV}/bin/activate && pip install -r requirements.txt

#============================================================================================#
# INSTALL DOCKER
#============================================================================================#

# Add Docker's official GPG key:
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

#$(. /etc/os-release && echo "$VERSION_CODENAME")
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
#================================================================================================#
#install java
#================================================================================================#
sudo apt install default-jre

#================================================================================================#
#install flink
#================================================================================================#

mkdir ~/Applications
cd ~/Applications
wget https://dlcdn.apache.org/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz
tar -xvzf  flink-1.18.1-bin-scala_2.12.tgz 

#================================================================================================#
# install vim
#================================================================================================#

mkdir -p ~/.vim/pack/vendor/start
git clone git@github.com:ap/vim-buftabline.git  ~/.vim/pack/vendor/start/vim-buftab-master
git clone https://github.com/ervandew/supertab.git ~/.vim/pack/vendor/start/supertab-master

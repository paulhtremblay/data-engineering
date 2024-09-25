set -e

#have to generate a ssh key
#copy this to repository
#ssh-keygen -t ed25519 -C "henry.tremblay@paccar.com"
#cat ~/.ssh/key.put then copy


#ssh-keygen -q -t rsa -N '' <<< $'\ny' >/dev/null 2>&1
#

sudo apt-get update
sudo apt install git-all

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

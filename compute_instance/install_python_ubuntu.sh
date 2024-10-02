set -e
sudo apt-get install build-essential libffi-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev liblzma-dev

for VERSION in 3.11.10 
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

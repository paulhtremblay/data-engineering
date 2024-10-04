
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

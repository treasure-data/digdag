#!/bin/bash -ex

apt-get -y update
apt-get -y install software-properties-common apt-transport-https wget git sudo
apt-get -y install ca-certificates

# Maven
apt-get -y install maven

# npm
apt-get -y install curl
curl -sL https://deb.nodesource.com/setup_12.x | bash -
apt-get -y install nodejs

# Docker required by CircleCI to execute docker command in Docker container
apt-get -y install docker.io
apt-get -y install docker-compose

# Postgres
sudo apt-key adv --recv-keys --keyserver keyserver.ubuntu.com 7FCC7D46ACCC4CF8 # To fix failure at add-apt-repository
add-apt-repository "deb https://apt.postgresql.org/pub/repos/apt/ bionic-pgdg main"
add-apt-repository "deb http://us.archive.ubuntu.com/ubuntu/ bionic main restricted"
wget --quiet -O - https://postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
apt-get -y update
apt-get -y install postgresql-11 postgresql-client-11
cp pg_hba.conf /etc/postgresql/11/main/
cp postgresql.conf /etc/postgresql/11/main/
/etc/init.d/postgresql start
sudo -u postgres createuser -s digdag_test
sudo -u postgres createdb -O digdag_test digdag_test

# Python
apt-get -y install python3 python3-pip
python3 -m pip install -U pip
python3 -m pip install -r requirements.txt -c constraints.txt

# Ruby
apt-get -y install ruby-full

# Redis
apt-get -y install redis-server

# Minio (S3)
wget -O /usr/local/bin/minio https://dl.minio.io/server/minio/release/linux-amd64/archive/minio.RELEASE.2019-01-23T23-18-58Z
chmod 777 /usr/local/bin/minio

# Redis
apt-get -y install redis-server
redis-server &

rm -rf /var/lib/apt/lists/*

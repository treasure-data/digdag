#!/bin/bash -ex
apt-get -y update
apt-get -y install software-properties-common apt-transport-https wget git

# Oracle JDK8
add-apt-repository ppa:webupd8team/java
echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
apt-get -y update
apt-get -y install oracle-java8-installer
apt-get -y install oracle-java8-set-default
rm -rf /var/cache/oracle-jdk8-installer

# Maven
apt-get -y install maven

# Postgres
add-apt-repository "deb https://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main"
wget --quiet -O - https://postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
apt-get -y update
apt-get -y install postgresql-9.5 postgresql-client-9.5
cp pg_hba.conf /etc/postgresql/9.5/main/
/etc/init.d/postgresql start
sudo -u postgres createuser -s digdag_test
sudo -u postgres createdb -O digdag_test digdag_test

# Python
apt-get -y install python python-pip python-dev
pip install sphinx recommonmark sphinx_rtd_theme

# Fake S3
apt-get -y install ruby
sudo gem install fakes3

rm -rf /var/lib/apt/lists/*

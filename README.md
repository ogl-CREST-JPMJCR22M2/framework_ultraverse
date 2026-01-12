# Ultraverse Tutorial

## Purge Existing MySQL 
```console
$ sudo apt purge mysql-server mysql-common mariadb-server mariadb-common
$ sudo apt autoremove
$ sudo apt autoclean
$ sudo rm -rf /var/lib/mysql /var/lib/mysql.* /var/log/mysql /etc/mysql
```


## Install Required Software
```console
$ sudo apt install build-essential cmake pkg-config bison flex libboost-all-dev libfmt-dev libspdlog-dev libgvc6 graphviz-dev doxygen libjemalloc-dev libmozjs-102-0 libmozjs-102-dev protoc-gen-go python3-dev libmysqlclient-dev build-essential wget python3 g++-12 clang-15 libc++-15-dev cmake libtbb-dev graphviz libgraphviz-dev libboost-all-dev libmysqlclient-dev libprotobuf-dev protobuf-compiler libfmt-dev libspdlog-dev golang-go
$ pip3 install sqlparse 
$ go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
$ sudo ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1    # if libasio.so.1 does not exist
```



## Install and Setup MySQL (MUST install only either MySQL)

```console
$ sudo apt install mysql-server mysql-client
```

Enable binary logging (check the actual including directory name specified in `/etc/mysql/my.cnf`).

```console
$ sudo vim /etc/mysql/mysql.conf.d/server.cnf
-------------------
   [mysqld]
   log-bin=myserver-binlog
   binlog_format=ROW
   binlog_row_image=FULL
   binlog_row_metadata=FULL
   binlog-checksum=NONE
   binlog_rows_query_log_events=ON
   max_binlog_size=300M
   plugin_load_add = ha_blackhole
   log_bin_trust_function_creators = 1
-------------------
```

Enable efficient large memory allocation
```console
$ sudo vim /etc/systemd/system/multi-user.target.wants/mysql.service
---------------
   Environment="LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2"
---------------
```

Activate jemalloc & binary logging
```console
$ sudo systemctl daemon-reload
$ sudo service mysql restart
```

Check that the value is not 'system', but 'jemalloc ...'
```console
$ sudo mysql -u root -p
> SHOW VARIABLES LIKE 'version_malloc_library'; 
```

Add the default 'admin' user for Benchbase
```bash
sudo mysql
> CREATE USER 'admin'@'localhost' IDENTIFIED BY 'password';
> GRANT ALL PRIVILEGES ON *.* TO 'admin'@'localhost';
CREATE DATABASE offchaindb
```


## Install Ultraverse
```console
$ git clone https://github.com/gogo9th/ultraverse-sigmod
$ cd ultraverse
$ git submodule init
$ git submodule update
$ mkdir build && cd build
$ sed -i "s/python3$/python3.$(echo "$(python3 --version)" | awk '{print $2}' | awk -F . '{print $2}')/g" ../src/CMakeLists.txt
$ CC=clang-15 CXX=clang++-15 cmake ..
$ make -j8
```

## Run BechBase Automatically
When running this automatic test, `/etc/mysql/mysql.conf.d/server.cnf` and `/etc/mysql/mysql.conf.d/server.cnf` should be empty, because the test runs fresh mysql binary and the existing configuration files cause a conflict.

```console
$ git clone -b fix/cfp-scenario https://github.com/ogl-CREST-JPMJCR22M2/framework_benchbase
$ cd framework_benchbase
$ ./make-mysql
```

```
$ cd ~/ultraverse/script/esperanza
$ source envfile
$ rm -rf runs cache
$ python3 cfp.py
```
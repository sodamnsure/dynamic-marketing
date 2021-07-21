## Redis安装

1. 安装GCC
```shell
yum -y install centos-release-scl
yum -y install devtoolset-9-gcc devtoolset-9-gcc-c++ devtoolset-9-binutils
scl enable devtoolset-9 bash
echo "source /opt/rh/devtoolset-9/enable" >> /etc/profile
source /etc/profile
```

2. 解压Redis源码
```shell
tar -zxvf redis-6.2.4.tar.gz
```

3. 编译安装
```shell
cd redis-6.2.4/
# 编译
make
# 迁出可执行文件
make install PREFIX=/opt/apps/redis
```

4. 配置环境变量
```shell
vim /etc/profile

export REDIS_HOME=/opt/apps/redis
export PATH=$PATH:$REDIS_HOME/bin

source /etc/profile
```

## Redis启动服务

1. 准备配置文件
```shell
cp ~/dev/soft/redis-6.2.4/redis.conf /opt/apps/redis
# 修改 redis.conf 中的服务器绑定地址
bind 0.0.0.0
# 修改后台启动
daemonize yes
# 配置密码
requirepass password
```

2. 启动服务
```shell
bin/redis-server redis.conf
```

## Redis关闭服务
```shell
# 当设置密码后，上面的关闭命令无效：
# 带密码输入：    
redis-cli -a password
#回车后输入：
shutdown
```
# DINGO SDK

## What's dingo-sdk?
The Dingo Store cpp sdk

First, you have prepared the Dingo Store environment, see the docs at https://github.com/dingodb/dingo-store

## Requirements

gcc 13

## How to build 

### Build dingo-eureka

refer https://github.com/dingodb/dingo-eureka

### Build SDK

#### Download the Submodule

In the source dir

```shell
git submodule sync --recursive

git submodule update --init --recursive
```

#### Build
In the source dir

```shell
mkdir build 

cd build

cmake -DTHIRD_PARTY_INSTALL_PATH=dingo-eureka-install-path ..

make -j 32
```
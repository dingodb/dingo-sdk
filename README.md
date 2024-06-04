# DINGO SDK

## What's dingo-sdk?
The Dingo Store cpp sdk

First, you have prepared the Dingo Store environment, see the docs at https://github.com/dingodb/dingo-store

## Requirements

gcc 13

## How to build 

### Download the Submodule

In the source dir

```shell
git submodule sync --recursive

git submodule update --init --recursive
```

### Build Third Party

In the source dir

```shell
cd third-party

cmake -S . -B build

cmake --build build -j 8
```

### Build SDK

In the source dir

```shell
cmake -S . -B build

cmake --build build -j 8
```
#!/bin/bash

set -e

: <<'COMMENT'
Set
export PATH="${HOME}/root/bin:${PATH}"
export LIBRARY_PATH="${HOME}/root/lib"
export CPATH="${HOME}/root/include"
export LD_LIBRARY_PATH="${HOME}/root/lib"
after this
COMMENT
# readline must find ncurses
export LIBRARY_PATH="${HOME}/root/lib"
# building flex needs yacc (bison)...
export PATH="${HOME}/root/bin:${PATH}"

depspath="${HOME}/deps"
prefix="${HOME}/root"

rm -rf ${depspath}
mkdir -p ${depspath}
cd ${depspath}

depslist=(
    "https://ftp.gnu.org/pub/gnu/ncurses/ncurses-6.1.tar.gz"
    "https://ftp.gnu.org/gnu/readline/readline-7.0.tar.gz"
    "ftp://ftp.gnu.org/gnu/bison/bison-3.1.tar.xz"
    # flex 2.6.4 doesn't compile on ubuntu 18 =)
    "https://github.com/westes/flex/releases/download/v2.6.3/flex-2.6.3.tar.lz")

for dep in "${depslist[@]}"
do
    cd $depspath
    archname=$(basename -- "${dep}")
    wget "${dep}"
    tar -x -f $archname
    # cut two extensions
    dirname="${archname%.*}"
    dirname="${dirname%.*}"

    cd $dirname
    additional_flags=""
    # build shared library
    if [[ $dirname = *"ncurses"* ]]; then
	additional_flags="--with-shared"
    fi

    # force linking with ncurses
    if [[ $dirname = *"readline"* ]]; then
	sed -i s/^SHLIB_LIBS=$/SHLIB_LIBS=-lncurses/ support/shobj-conf
    fi
    ./configure --prefix="${prefix}" $additional_flags && make -j4 && make install
done

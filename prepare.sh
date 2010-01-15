#!/bin/bash

# Prepare libraries for ParaTrac usage
# Debian/Ubuntu

PKGS="libfuse-dev libglib2.0-dev fuse-utils
python python-scipy python-numpy python-matplotlib python-networkx
python-gnuplot python-pydot"

sudo apt-get install $PKGS

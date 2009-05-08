#!/bin/sh

#############################################################################
# ParaTrac: Scalable Tracking Tools for Parallel Applications
# Copyright (C) 2009  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

# Due to the license reason, amCharts modules do not come with ParaTrac
# use this script to download and properly setup amCharts for you


url_amstock="http://www.amcharts.com/files/getfile.php?filename=amstock_1.1.7.1.zip"
url_amline="http://www.amcharts.com/files/getfile.php?filename=amline_1.6.3.0.zip"
url_amcolumn="http://www.amcharts.com/files/getfile.php?filename=amcolumn_1.6.3.0.zip"

MODULES="amstock amline amcolumn"
DUMMY_FILES="changelog.txt licence.txt readme.txt examples"
MODULES_DIR=modules
PLUGINS_DIR=$MODULES_DIR/plugins
FONTS_DIR=$MODULES_DIR/fonts
PATTERNS_DIR=$MODULES_DIR/patterns

# check directories for plugins and fonts
mkdir -p $PLUGINS_DIR
mkdir -p $FONTS_DIR
mkdir -p $PATTERNS_DIR

for module in $MODULES
do  
    url=url_$module
    if [ ! -f $module.zip ]; then
        wget -O $module.zip ${!url}
    fi
    unzip -o $module.zip
    if [ -f $module/$module.swf ]; then
        mv -f $module/$module.swf $MODULES_DIR/
    fi
    if [ -d $module/plugins ]; then
        mv -f $module/plugins/* $PLUGINS_DIR
    fi
    if [ -d $module/fonts ]; then
        mv -f $module/fonts/* $FONTS_DIR
    fi
    if [ -d $module/patterns ]; then
        mv -f $module/patterns/* $PATTERNS_DIR
    fi
    rm -rf $DUMMY_FILES $module $module.html
done

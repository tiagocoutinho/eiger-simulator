# Installation

Instructions to prepare software (eiger-simulator) and lima-camera-eiger.

## eiger-simulator

From within your python 3 environment:

Install latest from pypi:

```pip install eiger-simulator```

... or local git checkout:

```pip install .```

(add -e flag for development mode)

## Lima-camera-eiger

### Dependencies

#### Prepare conda dependencies

```terminal
conda install pytango -c tango-controls

conda install bitshuffle -c conda-forge
conda install lima-core lima-camera-eiger -c esrf-bcu
conda install cmake gxx_linux-64
```

Current official lima-camera-eiger is not able to connect to a specific http
port which is necessary if we want to start the eiger-simulator on a TCP port
other than port 80. So we have to build a lima-camera-eiger from tiagocoutinho
github fork until PR #2 is merged and the lima-camera-eiger conda package is
updated in the esrf-bcu conda channel.

Once this is done in conda we won't need the next steps.

#### Prepare bitshuffle

Could not install bitshuffle from conda conda-forge channel (dependencies conflict)

```terminal
git clone https://github.com/kiyo-masui/bitshuffle.git
cd bitshuffle
python setup.py build
pip install -e .  # not really necessary (lima-camera-eiger just needs a reference to bitshuffle.h)
```

*be sure to add directory where bitshuffle.h is to CPATH before compiling*


#### Prepare lima camera eiger

Grab the latest lima camera eiger from my repo until
PR #2 (https://github.com/esrf-bliss/Lima-camera-eiger/pull/2) is merged in
the official repo and conda packages for lima-camera-eiger are available:

```
git clone git@github.com:tiagocoutinho/lima-camera-eiger
cd lima-camera-eiger
cmake .
```

Edit CMakeCache.txt and enable python with `LIMA_ENABLE_PYTHON:BOOL=ON`

Had to patch:
* CMakeFiles/eiger.dir/link.txt
* CMakeFiles/python_module_limaeiger.dir/link.txt

* and replace -lGSL::gsl with -lgsl
* and replace -lGSL::gslcblas with -lgslcblas

Now we can do:
`cmake --build .`

copy liblimaeiger.so.1.8.0 to the conda environment lib dir
(ex: /homelocal/sicilia/miniconda3/envs/lima-eiger/lib/)

copy limaeiger.so to the conda environment python path dir
(ex: /homelocal/sicilia/miniconda3/envs/lima-eiger/lib/python3.7/site-packages/)



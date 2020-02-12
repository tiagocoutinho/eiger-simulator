#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""
import sys
from setuptools import setup, find_packages

TESTING = any(x in sys.argv for x in ["test", "pytest"])

requirements = ['fastapi', 'uvicorn', 'pyzmq>=17', 'click', 'h5py', 'bitshuffle']

setup_requirements = []
if TESTING:
    setup_requirements += ['pytest-runner']
test_requirements = ['pytest', 'pytest-cov']
extras_requirements = {
    'client' : ['requests']
}

with open('README.md') as f:
    description = f.read()

setup(
    author="ALBA controls team",
    author_email='controls@cells.es',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8'
    ],
    description="ALBA Eiger simulator",
    entry_points={
        'console_scripts': [
            'eiger-simulator=eigersim.server:main',
        ]
    },
    install_requires=requirements,
    license="GPL",
    long_description=description,
    long_description_content_type='text/markdown',
    include_package_data=True,
    keywords='alba, dectris, eiger, simulator',
    name='eiger-simulator',
    packages=find_packages(),
    package_data={},
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    python_requires='>=3.7',
    extras_require=extras_requirements,
    url='https://git.cells.es/controls/eiger-simulator',
    version='0.2.0'
)

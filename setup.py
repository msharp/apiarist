# Copyright 20014 Max Sharples
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# to publish on PyPI :
# $ python setup.py sdist bdist_wheel
# $ twine upload dist/*

import os
try:
    from setuptools import setup
    # arguments that distutils doesn't understand
    setuptools_kwargs = {
        'install_requires': [
            'boto>=2.6.0'
        ],
        'provides': ['apiarist']
    }
except ImportError:
    from distutils.core import setup
    setuptools_kwargs = {}

import apiarist

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    author='Max Sharples',
    author_email='maxsharples@gmail.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Topic :: System :: Distributed Computing',
    ],
    description='Python Hive query framework',
    license='Apache',
    long_description=long_description,
    long_description_content_type="text/markdown",
    name='apiarist',
    packages=[
        'apiarist'
    ],
    package_data={
        'apiarist': ['jars/*.jar']
    },
    url='http://github.com/msharp/apiarist',
    version=apiarist.__version__,
    **setuptools_kwargs
)

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

import os
try:
    from setuptools import setup
    # arguments that distutils doesn't understand
    setuptools_kwargs = {
        'install_requires': [
            'boto>=2.2.0'
        ],
        'provides': ['apiarist']
    }
except ImportError:
    from distutils.core import setup
    setuptools_kwargs = {}

import apiarist

setup(
    author='Max Sharples',
    author_email='maxsharples@gmail.com',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: System :: Distributed Computing',
    ],
    description='Python Hive query framework',
    license='Apache',
    long_description=open(os.path.abspath(os.path.join(os.path.dirname(__file__),'README.md'))).read(),
    name='apiarist',
    packages=[
        'apiarist'
    ],
    url='http://github.com/msharp/apiarist',
    version=apiarist.__version__,
    **setuptools_kwargs
)

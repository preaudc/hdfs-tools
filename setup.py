import os
import setuptools

name = 'hdfs-tools'
version = '1.0.0.dev0'
description = 'Miscellaneous python tools for HDFS administration'

setuptools.setup(
    name=name,
    version=version,
    author='Christophe PRÉAUD',
    author_email='contact-gitlab@aquilae.fr',
    description=description,
    url='https://github.com/preaudc/hdfs-tools',
    package_dir = {'': 'src/main/python'},
    packages=setuptools.find_packages(where='src/main/python'),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: Apache License 2.0',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    install_requires = ['pyyaml'],
    scripts=[
        'src/main/scripts/print_block_replica_status',
        'src/main/scripts/get_hadoop_site_keys'
    ]
)

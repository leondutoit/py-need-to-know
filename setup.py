#!/usr/bin/env python

from setuptools import setup

setup(
    name='pyneedtoknow',
    version='0.1.0',
    description='Python client for pg-need-to-know',
    author='Leon du Toit',
    author_email='dutoit.leon@gmail.com',
    url='https://github.com/leondutoit/py-need-to-know',
    packages=['pyneedtoknow'],
    package_data={
        'pyneedtoknow': [
            'tests/*.py'
        ]
    },
)

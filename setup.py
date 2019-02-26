#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name = "dwgenerator",
    version = "0.1",
    url = "https://github.com/TopOfMinds/dw-generator",
    description="Data Warehouse Generator",
    entry_points='''
        [console_scripts]
        dwgenerator=dwgenerator.cli:cli
    ''',
    packages = find_packages(),
    install_requires=[
        'Click',
        'Jinja2',
        'colorama' ],
    setup_requires=['setuptools_git'],
    include_package_data=True,
)

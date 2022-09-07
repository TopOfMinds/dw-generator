#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_namespace_packages

setup(
    name = "dwgenerator",
    version = "0.1",
    url = "https://github.com/TopOfMinds/dw-generator",
    description="Data Warehouse Generator",
    packages = find_namespace_packages(),
    setup_requires=['setuptools_git'],
    include_package_data=True,
)

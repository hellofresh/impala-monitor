#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

requirements = [
    'statsd==3.2.1',
    'requests',
    'click',
    'apscheduler'
    'coverage',
    'nose'
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='impala-monitor',
    version='0.0.1',
    description="Monitor for Impala",
    author="Sergio Sola",
    author_email='ss@hellofresh.com',
    url='https://github.com/hellofresh/impala-monitor',
    packages=[
        'impala_monitor',
    ],
    package_dir={'impala_monitor': 'impala_monitor'},
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='impala monitor',
    test_suite='tests',
    tests_require=test_requirements
)

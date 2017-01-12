import os
import platform
import setuptools
from setuptools import setup, find_packages


REQUIRED_PACKAGES = [
    'luigi==2.3.1',
    'slackweb==1.0.5',
    'csvkit==0.9.1',
    'xmltodict==0.9.2',
    'retrying==1.3.3',
    'Jinja2==2.8'
]


setuptools.setup(
    name='oz-analytics-dmp-etl',
    version='1.0.23',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=REQUIRED_PACKAGES,
    test_suite='nose.collector',
    zip_safe=False
)

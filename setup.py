from setuptools import setup, find_packages
import glob
import os
import sys

from taca_ngi_pipeline import __version__

try:
    with open("requirements.txt", "r") as f:
        install_requires = [x.strip() for x in f.readlines()]
except IOError:
    install_requires = []

try:
    with open("dependency_links.txt", "r") as f:
        dependency_links = [x.strip() for x in f.readlines()]
except IOError:
    dependency_links = []


setup(name='taca-ngi-pipeline',
    version=__version__,
    description='Plugin for the TACA tool, bringing functionality for the '\
        'ngi_pipeline to the table',
    long_description='This package is a plugin that adds a set of subcommands '\
        'to the TACA tool. The subcommands are useful when interacting with '\
        'the ngi_pipeline. The TACA tool and the ngi_pipeline are used in the '\
        'day-to-day tasks of bioinformaticians in National Genomics '\
        'Infrastructure in Sweden.',
    keywords='bioinformatics',
    author='Guillermo Carrasco, Pontus Larsson',
    author_email='guille.ch.88@gmail.com',
    url='http://taca.readthedocs.org/en/latest/',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    scripts=glob.glob('scripts/*.py'),
    include_package_data=True,
    zip_safe=False,

    entry_points={
        'taca.subcommands': [
            'deliver = taca_ngi_pipeline.deliver.cli:deliver',
        ]
    },
    install_requires=install_requires,
    dependency_links=dependency_links
)

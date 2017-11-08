# -*- coding: utf-8 -*-
import imp

from setuptools import setup, find_packages

# Import metadata. Normally this would just be:
#
#     from shiftmanager import metadata
#
# However, when we do this, we also import `shiftmanager/__init__.py'. If this
# imports names from some other modules and these modules have third-party
# dependencies that need installing (which happens after this file is run), the
# script will crash. What we do instead is to load the metadata module by path
# instead, effectively side-stepping the dependency problem. Please make sure
# metadata has no dependencies, otherwise they will need to be added to
# the setup_requires keyword.
metadata = imp.load_source('metadata', 'shiftmanager/metadata.py')


setup(
    name=metadata.package,
    version=metadata.version,
    author=metadata.authors[0],
    author_email=metadata.emails[0],
    maintainer=metadata.authors[0],
    maintainer_email=metadata.emails[0],
    url=metadata.url,
    description=metadata.description,
    # Find a list of classifiers here:
    # <http://pypi.python.org/pypi?%3Aaction=list_classifiers>
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    packages=find_packages(),
    package_data={'shiftmanager': ['*.json']},
    install_requires=[
        "boto>=2.38.0",
        "psycopg2>=2.5.4",
        "sqlalchemy-redshift>=0.7.0",
        "sqlalchemy-views>=0.2",
    ]
)

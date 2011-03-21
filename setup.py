#!python

import os.path, sys
from setuptools import setup, find_packages

sys.path.insert(0, os.path.abspath("src"))
from whoosh import __version__, versionstring

setup(
	name = "Whoosh",
	version = versionstring(),
	package_dir = {'': 'src'},
	packages = ["whoosh", "whoosh.filedb", "whoosh.lang", "whoosh.qparser", "whoosh.support"],
	
	author = "Matt Chaput",
	author_email = "matt@whoosh.ca",
	
	description = "Fast, pure-Python full text indexing, search, and spell checking library.",
    long_description = open("README.txt").read(),

	license = "Two-clause BSD license",
	keywords = "index search text spell",
	url = "http://bitbucket.org/mchaput/whoosh",
	
	zip_safe = True,
	test_suite = "nose.collector",
	
	classifiers = [
	"Development Status :: 5 - Production/Stable",
	"Intended Audience :: Developers",
	"License :: OSI Approved :: BSD License",
	"Natural Language :: English",
	"Operating System :: OS Independent",
	"Programming Language :: Python :: 2.5",
	"Topic :: Software Development :: Libraries :: Python Modules",
	"Topic :: Text Processing :: Indexing",
	],
)

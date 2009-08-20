#!python

import os.path, sys
from setuptools import setup, find_packages

sys.path.insert(0, os.path.abspath("src"))
from whoosh import __version__, versionstring

setup(
	name = "Whoosh",
	version = versionstring(),
	package_dir = {'': 'src'},
	packages = ["whoosh", "whoosh.filedb", "whoosh.lang", "whoosh.support"],
	
	author = "Matt Chaput",
	author_email = "matt@whoosh.ca",
	description = "Fast, pure-Python full text indexing, search, and spell checking library.",
	
    long_description = """
Whoosh is a fast, pure-Python indexing and search library. Programmers
can use it to easily add search functionality to their applications and
websites. Because Whoosh is pure Python, you don't have to compile or
install a binary support library and/or make Python work with a JVM, yet
Whoosh is still very fast at indexing and searching. Every part of how
Whoosh works can be extended or replaced to meet your needs exactly.

This software is licensed under the terms of the Apache License version 2.
See LICENSE.txt for information.

The primary source of information is the main Whoosh web site:
http://whoosh.ca/

You can check out the latest version of the source code from subversion at:
http://svn.whoosh.ca/projects/whoosh/trunk/
""",

	license = "Apache 2.0",
	keywords = "index search text spell",
	url = "http://whoosh.ca",
	
	zip_safe = True,
	test_suite = "tests",
	
	classifiers = [
	"Development Status :: 3 - Alpha",
	"Intended Audience :: Developers",
	"License :: OSI Approved :: Apache Software License",
	"Natural Language :: English",
	"Operating System :: OS Independent",
	"Programming Language :: Python :: 2.5",
	"Topic :: Software Development :: Libraries :: Python Modules",
	"Topic :: Text Processing :: Indexing",
	],
	
)

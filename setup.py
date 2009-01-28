#!python

from setuptools import setup, find_packages

setup(
	name = "Whoosh",
	version = "0.1",
	packages = find_packages(exclude = ["tests"]),
	
	author = "Matt Chaput",
	author_email = "matt@whoosh.ca",
	description = "Fast, pure-Python full text indexing, search, and spell checking library",
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

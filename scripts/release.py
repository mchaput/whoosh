import sys
from ConfigParser import ConfigParser
from optparse import OptionParser
from os import system

# Script to build and upload a release of Whoosh to PyPI and build
# and upload the 

def build_docs():
    system("python setup.py build_sphinx")

def upload_docs(user, server, base, version, build=True, latest=True):
    opts = {"user": user,
            "srv": server,
            "base": base,
            "ver": version}

    system('ssh %(user)s@%(srv)s "mkdir %(base)s/%(ver)s"' % opts)
    system("scp -r docs/build/html/* %(user)s@%(srv)s:%(base)s/%(ver)s" % opts)
    system('ssh %(user)s@%(srv)s "cd %(base)s;ln -s %(ver)s latest"' % opts)


def upload_pypi(tag=None):
    system("python setup.py sdist bdist_egg upload")
    if tag:
        system("svn copy http://svn.whoosh.ca/projects/whoosh/trunk http://svn.whoosh.ca/projects/whoosh/tags/%s" % tag)


if __name__ == '__main__':
    from whoosh import __version__

    version = ".".join(str(n) for n in __version__)

    parser = OptionParser()
    parser.add_option("-c", "--config", dest="configfile",
                      help="Configuration file",
                      metavar="INIFILE",
                      default="whoosh.ini")
    
    parser.add_option("-d", "--no-docs", dest="dodocs",
                      help="Don't build or upload docs",
                      action="store_false",
                      default=True)
    
    parser.add_option("-D", "--no-build-docs", dest="builddocs",
                      help="Skip building docs",
                      action="store_false",
                      default=True)
    
    (options, args) = parser.parse_args()
    
    cp = ConfigParser()
    cp.read(options.configfile)
    
    if options.dodocs:
        upload_docs(cp.get("username"), cp.get("server"), cp.get("docbase"), version,
                    build=options.builddocs)

    upload_pypi(tag=version)

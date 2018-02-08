from pip.download import PipSession
from pip.req import parse_requirements
from setuptools import find_packages, setup

from akfak import __version__ as akfak_version

reqs = parse_requirements('requirements.txt', session=PipSession())
requirements = [str(req.req) for req in reqs]

setup(
    name='akfak',
    author='Casey Weed',
    author_email='cweed@caseyweed.com.com',
    description='Burrow clone',
    url='https://github.com/battleroid/akfak',
    version=akfak_version,
    packages=find_packages(),
    install_requires=requirements,
    entry_points="""
        [console_scripts]
        akfak=akfak.cli:cli
    """
)

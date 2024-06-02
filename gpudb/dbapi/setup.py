"""Setup script for Kinetica DB API."""
import setuptools
from gpudb.dbapi import __version__

with open("README.md", "r") as readme_file:
    long_description = readme_file.read()

setuptools.setup(
    name="kinetica_dbapi",
    version=__version__,
    author="Kinetica",
    url="https://github.com/kineticadb/kinetica-api-python",
    description="A Python DB API interface for Kinetica.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Development Status :: 4 - Beta",
        "Natural Language :: English",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries",
        "License :: OSI Approved :: MIT License",
        "Typing :: Typed",
    ],
    python_requires=">=3.8",
    install_requires=[
        "gpudb==7.2.0.4",
    ]
)

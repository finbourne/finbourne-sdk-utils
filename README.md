![LUSID_by_Finbourne](./resources/Finbourne_Logo_Teal.svg)

# Python tools for LUSID

This package contains a set of utility functions and a Command Line Interface (CLI) for interacting with [LUSID by FINBOURNE](https://www.finbourne.com/lusid-technology). To use it you'll need a LUSID account. [Sign up for free at lusid.com](https://www.lusid.com/app/signup)


![PyPI](https://img.shields.io/pypi/v/lusidtools?color=blue)
![Daily build](https://github.com/finbourne/lusid-python-tools/workflows/Daily%20build/badge.svg) 
![Build and test](https://github.com/finbourne/lusid-python-tools/workflows/Build%20and%20test/badge.svg)
![Commit hook](https://github.com/finbourne/lusid-python-tools/workflows/commit-hook/badge.svg)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=finbourne_lusid-python-tools&metric=alert_status)](https://sonarcloud.io/dashboard?id=finbourne_lusid-python-tools)

For more details see the lusid-python-tools [wiki](https://github.com/finbourne/lusid-python-tools/wiki).

## Installation

The PyPi package for lusid-python-tools can installed globally on your machine using the following command:

```sh
$ pip install finbourne-sdk-utils
```

or if you are running as a non privileged user you may prefer to install specifically for your user account:

```sh
$ pip install --user finbourne-sdk-utils
```

## CLI usage

Make sure that the Python `bin` folder is on your search path before trying the following examples.  This is typically found under following locations:

* Windows: C:\Users\[userid]\AppData\Local\Programs\Python\[version]
* macOS: /Users/[userid]/Library/Python/[version]/bin
* Linux: /usr/local/bin/python

## Upgrading

To upgrade lusidtools run one of the commands below 

```sh
$ pip install finbourne-sdk-utils -U
```

or

```sh
$ pip install finbourne-sdk-utils -U --user
```

from setuptools import setup, find_packages  # noqa: H301

version = {}
with open("./__version__.py") as fp:
    exec(fp.read(), version)

setup(
    name="finbourne-sdk-utils",
    version=version["__version__"],
    description="Python utilities for LUSID V2 SDK",
    url="https://github.com/finbourne/finbourne-sdk-utils",
    author="FINBOURNE Technology",
    author_email="engineering@finbourne.com",
    license="MIT",
    keywords=["FINBOURNE", "LUSID", "LUSID SDK", "python"],
    packages=find_packages(exclude=["tests*"]),
    install_requires=[
        "urllib3>=1.26.9",
        "requests>=2.27.1",
        "coloredlogs>=14.0",
        "detect_delimiter>=0.1",
        "flatten-json>=0.1.7",
        "pandas>=1.1.4",
        "PyYAML>=5.4",
        "tqdm>=4.52.0",
        "openpyxl>=3.0.7",
        "xlrd~=1.2",
        "pytz>=2019.3",
        "IPython==8.32",
        "lusid-sdk>=2",
    ],
    include_package_data=True,
    python_requires=">=3.7",
)

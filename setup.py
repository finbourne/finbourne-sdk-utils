from setuptools import setup, find_packages  # noqa: H301

version = {}
with open("./__version__.py") as fp:
    exec(fp.read(), version)

setup(
    name="finbourne-sdk-utils",
    version=version["__version__"],
    description="Python utilities for working with the LUSID SDK",
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
        "IPython>=7.31.1",
        "lusid-sdk>=2",
    ],
    include_package_data=True,
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "upsert_portfolios=finbourne_sdk_utils.apps.upsert_portfolios:main",
            "finbourne_sdk_utils=finbourne_sdk_utils.commands.commands:main",
            "upsert_instruments=finbourne_sdk_utils.apps.upsert_instruments:main",
            "upsert_holdings=finbourne_sdk_utils.apps.upsert_holdings:main",
            "upsert_quotes=finbourne_sdk_utils.apps.upsert_quotes:main",
            "upsert_transactions=finbourne_sdk_utils.apps.upsert_transactions:main",
            "flush_transactions=finbourne_sdk_utils.apps.flush_transactions:main",
        ],
    },
)

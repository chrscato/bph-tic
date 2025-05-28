from setuptools import setup, find_packages

setup(
    name="tic_mrf_scraper",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests>=2.31.0",
        "ijson>=3.2.0",
        "pyarrow[parquet]>=12.0.0",
        "s3fs>=2024.4.0",
        "boto3>=1.29.0",
        "PyYAML>=6.0",
        "backoff>=2.2.1",
        "structlog>=23.1.0",
    ],
    python_requires=">=3.9",
) 
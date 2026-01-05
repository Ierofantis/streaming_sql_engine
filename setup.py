"""
Setup script for streaming-sql-engine
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

try:
    with open("requirements.txt", "r", encoding="utf-8") as fh:
        requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]
except FileNotFoundError:
    # Fallback to basic requirements if requirements.txt doesn't exist
    requirements = [
        "sqlglot>=23.0.0",
        "polars>=0.19.0",
    ]

setup(
    name="streaming-sql-engine",
    version="0.1.31",
    author="Ierofantis",
    author_email="teopanta1986@gmail.com",
    description="Streaming SQL Engine: Lightweight Cross Data Source Integration for Resource Constraint Environments",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Ierofantis/streaming_sql_engine/",
    # Package structure: files are in root directory
    packages=["streaming_sql_engine", "streaming_sql_engine.core", "streaming_sql_engine.operators", "streaming_sql_engine.polars_utils", "streaming_sql_engine.storage"],
    package_dir={"streaming_sql_engine": "."},
    include_package_data=True,
    exclude_package_data={
        "": ["setup.py", "*.pyc", "__pycache__", "build/*", "dist/*", "*.egg-info/*"],
        "streaming_sql_engine": ["setup.py", "test/*", "test_data/*", "build/*", "dist/*"]
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "db": [
            "psycopg2-binary>=2.9.0",
            "pymysql>=1.0.0",
            "DBUtils>=3.0.0",
            "pymongo>=4.0.0",
        ],
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
        ],
        "all": [
            "psycopg2-binary>=2.9.0",
            "pymysql>=1.0.0",
            "DBUtils>=3.0.0",
            "pymongo>=4.0.0",
        ],
    },
    keywords="sql streaming join database query engine",
    project_urls={
        "Bug Reports": "https://github.com/Ierofantis/streaming_sql_engine/issues",
        "Source": "https://github.com/Ierofantis/streaming_sql_engine/",
        "Documentation": "https://github.com/Ierofantis/streaming_sql_engine/blob/master/README.md",
        "Scientific Paper": "https://github.com/Ierofantis/streaming_sql_engine/blob/master/Streaming_SQL_Engine_Paper.pdf",
    },
)


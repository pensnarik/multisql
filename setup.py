from setuptools import setup

setup(
    name="multisql",
    version="1.0.1",
    description="Executing SQL statements on multiple PostgreSQL servers",
    py_modules=["multisql"],
    install_requires=["psycopg2"]
)

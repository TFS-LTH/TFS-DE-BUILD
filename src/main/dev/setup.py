from setuptools import setup, find_packages

setup(
    name="lemontree",
    version="1.0.0",
    packages=find_packages(include=["com", "com.*"]),
    description="Lemontree AWS Glue Jobs",
    author="LemonTree",
    author_email="you@example.com",
    python_requires=">=3.9",
)

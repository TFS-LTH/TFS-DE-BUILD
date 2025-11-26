from setuptools import find_packages, setup

setup(
    name="lemontree",
    version="1.0.0",
    packages=find_packages(include=["com", "com.*"]),
    description="Lemontree AWS Glue Jobs",
    author="LemonTree",
    author_email="you@example.com",
    python_requires=">=3.9",
    package_data={
        "com.lemontree": [
            "**/*.yaml",
            "**/*.yml",
            "**/*.json",
            "**/*.conf",
            "**/*.txt",
        ],
    },
)

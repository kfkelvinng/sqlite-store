from setuptools import setup, find_packages

setup(
    name="sqlite-store",
    version="0.1.0",
    description="Sqlite Store",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/kfkelvinng/sqlite-store",
    author="Kelvin Ng",
    packages=find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.0",
)

from setuptools import setup, find_packages

setup(
    name="superlake",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.5.3",
        "delta-spark>=3.3.0",
        "pandas>=1.0.0",
        "pytz",
        "applicationinsights",
        "loguru",
        "mypy",
        "pytest",
    ],
    author="Loic Magnien",
    author_email="loic.magnien@gmail.com",
    description="A modern, intuitive Python package for data lakehouse operations",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/loicmagnien/superlake",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.7",
) 
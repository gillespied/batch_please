from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


setup(
    name="batch_please",
    version="0.1.0",
    author="David Gillespie",
    author_email="git.atypical244@slmail.me",
    description="A package for batch processing with sync and async capabilities",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gillespied/batch_please",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    install_requires=[
        "tqdm",
    ],
    extras_require={
        "dev": [
            "pytest",
            "pytest-asyncio",
            "mkdocs",
            "mkdocs-material",
            "mkdocstrings[python]",
            "pre-commit",
        ],
    },
)

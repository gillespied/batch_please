from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


setup(
    name="batch_please",
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
    use_scm_version={
        "write_to": "src/batch_processors/_version.py",
    },
    setup_requires=["setuptools_scm"],
    install_requires=[
        "tqdm",
    ],
    extras_require={
        "dev": [
            "build",
            "pytest",
            "pytest-asyncio",
            "mkdocs",
            "mkdocs-material",
            "mkdocstrings[python]",
            "pre-commit",
        ],
    },
)

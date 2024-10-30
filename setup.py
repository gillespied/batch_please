from setuptools import find_packages, setup

setup(
    name="batch_processors",
    version="0.1.0",
    author="David Gillespie",
    author_email="davidjamesgillespie@gmail.com",
    description="A package for batch processing with sync and async capabilities",
    long_description=open("README.md").read(),
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
        "asyncio",
    ],
    extras_require={
        "dev": [
            "setuptools",
            "wheel",
            "pytest",
            "pytest-asyncio",
        ],
    },
)

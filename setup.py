from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="awsjsondataset",
    use_scm_version=True,
    description="JSON data wrapper with methods to AWS services.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Gregory Lindsey",
    author_email="gclindsey@gmail.com",
    url="https://github.com/abk7777/awsjsondataset",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11"
    ],
    python_requires=">=3.8,<4.0",
    install_requires=[],
    extras_require={
        "dev": [
            "ipykernel",
            "python-dotenv",
            "pytest",
            "pytest-cov",
            "mypy",
            "pre-commit",
            "tox",
            "black",
        ]
    },
    setup_requires=["setuptools_scm"],
)

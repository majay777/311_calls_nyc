from setuptools import find_packages, setup

setup(
    name="dagster_files",
    packages=find_packages(exclude=["dagster_files_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        ""
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)

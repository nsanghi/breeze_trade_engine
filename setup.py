from setuptools import setup, find_packages

setup(
    name="breeze_engine",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "breeze-connect",
        "python-dotenv",
        "numpy",
        "scipy",
        "matplotlib",
        "pandas",
        # List your package dependencies here
    ],
)

from setuptools import setup, find_packages

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="Content-Aware-Anime-Recommendation-System",
    version="1.0.0",
    author="Om Patil",
    packages=find_packages(),
    install_requires=required,
)
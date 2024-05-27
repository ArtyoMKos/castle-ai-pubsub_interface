from setuptools import setup
import os

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='pubsub_interface',
    version=os.getenv('CI_COMMIT_TAG', '0.0.1').removeprefix("v"),
    description='Podcastle castle-ai google pubsub interface',
    long_description='This package created for working and interacting with google pubsub services using'
                     'Google Cloud Pub/Sub.',
    packages=[''],  # Todo: fill
    author="Artyom Kosakyan",
    author_email="artyom@podcastle.ai",
    install_requires=required,
    license="MIT",
    python_requires='>=3.6',
    classifiers=[
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3"
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)

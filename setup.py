from mypyc.build import mypycify
from setuptools import setup

setup(
    name="sverka",
    packages=["sverka"],
    ext_modules=mypycify(
        [
            "sverka/__init__.py",
            "sverka/foo.py",
        ],
        verbose=True,
    ),
)

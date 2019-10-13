from setuptools import setup, find_packages

setup(
    name='Giraffe',
    version='1.0',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    license='Nothing here so far',
    author='Boris Milner',
    author_email='boris.milner@gmail.com',
    long_description=open('readme.md').read()
)

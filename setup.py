import setuptools

setuptools.setup(
    name='dojobs',
    version='0.0.1',
    description='Utility for quickly invoking many scripts.',
    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': [
            'dojobs = src.cli:main',
       ],
    },
    install_requires=[],
    tests_require=[]
)


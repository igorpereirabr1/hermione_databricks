from setuptools import setup, find_packages
from distutils.util import convert_path
import re
import os


main_ns = {}
ver_path = convert_path('hermione_databricks/version.py')
with open(ver_path) as ver_file:
    exec(ver_file.read(), main_ns)

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

__version__ = main_ns['__version__']

setup(
    name='hermione-databricks',
    version=main_ns['__version__'],
    author='igor.pereira.br@gmail.com',
    author_email='ju195@cummins.com',
    url='https://github.com/igorpereirabr1/hermione_databricks',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'Topic :: Software Development'
      ],
      keywords='machine learning mlops devops artificial intelligence',
      license='Apache License 2.0',
    install_requires=[
        'Click',
        'conda'
    ],
    entry_points='''
        [console_scripts]
        hermione_databricks=hermione_databricks.cli.cli:cli
    ''',
    python_requires='>=3.6',
    package_data={'hermione_databricks': ['databricks_file_text/*']}
)

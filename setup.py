from setuptools import setup, find_packages
import re
import os

exec(open('hermione_databricks/_version.py').read())

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='hermione-databricks',
    version=__version__,
    author='igor.pereira.br@gmail.com',
    author_email='ju195@cummins.com',
    url='https://github.com/igorpereirabr1/hermione_databricks',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
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

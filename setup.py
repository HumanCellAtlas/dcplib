import os
from setuptools import setup, find_packages

install_requires = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements.txt"))]

setup(name='dcplib',
      version='1.6.6',
      description='Modules shared among multiple Data Coordination Platform components.',
      url='http://github.com/HumanCellAtlas/dcplib',
      author='Sam Pierson',
      author_email='spierson@chanzuckerberg.com',
      license='MIT',
      packages=find_packages(exclude=['tests']),
      zip_safe=False,
      install_requires=install_requires,
      platforms=['MacOS X', 'Posix'],
      test_suite='test',
      classifiers=[
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: POSIX',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6'
      ]
      )

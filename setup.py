#!/usr/bin/env python
#
# INDI-Tools/setup.py
#
# Author: Daniel Clark, 2016

'''
This is the setup.py installation script to install the awsutils
package
'''

# Import packages
from setuptools import find_packages, setup

# Use disutils' setup function to install the package
setup(name='INDI-Tools',
      version='0.0.7',
      description='Python utilities developed by FCP-INDI',
      author='Daniel Clark',
      author_email='daniel.clark@childmind.org',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'Programming Language :: Python :: 3'
      ],
      url='https://github.com/FCP-INDI/INDI-Tools',
      packages=find_packages(exclude=['test*']),
      install_requires=['botocore', 'boto3'])

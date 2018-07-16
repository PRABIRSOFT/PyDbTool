from setuptools import setup

setup(name='PyDriver',
      version='0.1',
      description='Python all in one data-driver',
      url='',
      author='Prabir Ghosh',
      author_email='mymail.prabir@gmail.com',
      license='MIT',
      packages=['PyDriver'],
      install_requires=["pandas","psycopg2","cassandra-driver"],
      zip_safe=False)
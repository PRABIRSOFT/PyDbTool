from setuptools import setup

def readme():
    with open('README.md') as f:
        return f.read()

setup(name='PyDbTool',
      version='2.0.1',
      description='Python all in one data-driver',
      long_description=readme(),
      url='',
      author='Prabir Ghosh',
      author_email='mymail.prabir@gmail.com',
      license='MIT',
      packages=['PyDbTool'],
      install_requires=["pandas","psycopg2","cassandra-driver","pymongo"],
      zip_safe=False)
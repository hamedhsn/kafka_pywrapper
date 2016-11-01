from setuptools import setup, find_packages

__author__ = 'Hamed'

requirements = [
    'pykafka',
    'avro-python3'
]

setup(
    name='kafka_pywrapper',
    version='1.0.0',
    description='kafka python wrapper - support JSON, AVRO message',
    author='Hamed',
    maintainer='Hamed',
    maintainer_email='hamedhsn@gmail.com',
    packages=find_packages(),
    install_requires=requirements,
    data_files=[('cfg', ['cfg/dm.cfg'])],
    include_package_data=True,
    license='''MIT Licence. 2016'''
)

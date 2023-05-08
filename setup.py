from setuptools import setup

setup(
    name='demo-ingestion',
    version='1.1',
    packages=['demo', 'demo.metadata_setup', 'demo.pipelines', 'demo.TataDigital'],
    url='https://github.com/vijaypavann-db/demo-ingestion',
    license='',
    author='India Global Delivery Center Team',
    author_email='india_delivery@databricks.com',
    description='Demo Ingestion framework',
    setup_requires=['virtualenv', 'wheel', 'check-wheel-contents'],
    python_requires='>=3'
)

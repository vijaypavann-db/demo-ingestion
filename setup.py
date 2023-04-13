from setuptools import setup

setup(
    name='demo-ingestion',
    version='1.0',
    packages=['demo', 'demo.metadata_setup', 'demo.pipelines', 'demo.TataDigital', 'com', 'com.db', 'com.db.fw', 'com.db.fw.metadata', 'com.db.fw.etl', 'com.db.fw.etl.core',
              'com.db.fw.etl.core.common', 'com.db.fw.etl.core.writers', 'com.db.fw.etl.core.readers', 'com.db.fw.etl.core.samples',
              'com.db.fw.etl.core.pipeline', 'com.db.fw.etl.core.exception', 'com.db.fw.etl.core.processor'],
    url='https://github.com/vijaypavann-db/demo-ingestion',
    license='',
    author='India Global Delivery Center Team',
    author_email='india_delivery@databricks.com',
    description='Demo Ingestion framework',
    setup_requires=['virtualenv', 'wheel', 'check-wheel-contents'],
    python_requires='>=3'
)

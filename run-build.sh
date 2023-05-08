pip install dependencies/pyspark_ingestion-1.1-py3-none-any.whl  --force-reinstall

rm -r build
rm -r dist

python setup.py bdist_wheel
check-wheel-contents dist

echo "\n demo-ingestion build Success!!!"

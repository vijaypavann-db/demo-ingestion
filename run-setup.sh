pip install --use-pep517 -r requirements.txt

pip install dependencies/pyspark_ingestion-1.0-py3-none-any.whl  --upgrade --target .

rm -r build
rm -r dist

python setup.py bdist_wheel
check-wheel-contents dist

echo "\n demo-ingestion build Success!!!"

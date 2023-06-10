pip install --upgrade pip
pip install --use-pep517 -r requirements_local.txt

pip install dependencies/pyspark_ingestion-1.1-py3-none-any.whl  --force-reinstall

rm -r -f build
rm -r -f dist

python setup.py bdist_wheel
check-wheel-contents dist

echo "\n demo-ingestion build Success!!!"

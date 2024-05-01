
pushd .
cd /D "%~dp0"
cd ..\..\
cd src\
alembic revision --autogenerate -m %1
popd

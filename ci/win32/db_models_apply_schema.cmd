
pushd .
cd /D "%~dp0"
cd ..\..\
cd src\
alembic upgrade head
popd

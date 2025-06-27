@echo off
set "ROOT=%~dp0"
pushd %ROOT%src

%ROOT%venv\Scripts\python.exe -O -m sverka.foo
popd

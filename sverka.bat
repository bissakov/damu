@echo off
title Sverka

set "ROOT=%~dp0"
pushd %ROOT%src

%ROOT%venv\Scripts\python.exe -OO -m sverka.main
popd
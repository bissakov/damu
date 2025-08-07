@echo off
title Zaneseniya

set "ROOT=%~dp0"
pushd %ROOT%src

%ROOT%venv\Scripts\python.exe -OO -m zanesenie.main
popd
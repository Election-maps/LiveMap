@echo off
title WhatsApp Group Live Map
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0server.ps1"
pause

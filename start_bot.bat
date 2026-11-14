@echo off
title AlphaStrike v4 Engine
cd /d C:\Users\dangl\alphastrike_bot
:loop
echo [%date% %time%] Starting AlphaStrike v4...
uvicorn main:app --host 127.0.0.1 --port 8000
echo [%date% %time%] Engine stopped or crashed. Restarting in 5 seconds...
timeout /t 5 /nobreak >nul
goto loop
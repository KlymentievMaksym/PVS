@echo off
echo [START] Running Setup...
call .\scripts\setup.bat

echo.
echo [START] Crashing Test Data..
call .\scripts\crash_test.bat

echo.
echo [DONE] All scripts executed.
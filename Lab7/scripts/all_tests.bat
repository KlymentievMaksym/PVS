@echo off
echo [START] Running Setup...
call .\scripts\setup.bat

echo.
echo [START] Inserting Test Data..
call .\scripts\insert_test.bat

echo.
echo [DONE] All scripts executed.
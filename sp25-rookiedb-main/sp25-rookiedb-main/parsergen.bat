@echo off
rmdir /s /q src\main\java\edu\berkeley\cs186\database\cli\parser 2>nul
jjtree RookieParser.jjt
javacc src\main\java\edu\berkeley\cs186\database\cli\parser\RookieParser.jj
del /f src\main\java\edu\berkeley\cs186\database\cli\parser\RookieParser.jj 2>nul
echo Parser generation completed.
pause
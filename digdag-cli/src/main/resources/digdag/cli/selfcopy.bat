@echo off
set i=0
:repeat
  if %i% leq 30 (goto move) else goto :done
:move
  ping 1.1.1.1 -n 1 -w 500 >NUL 2>NUL
  move /Y %1 %2 >NUL 2>NUL
  set /a "i = i + 1"
  if exist %1 goto repeat
:done
del %0

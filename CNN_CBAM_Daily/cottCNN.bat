cd /d D:/Nicole/python/cottCNN
call activate cottvenv
REM pause
call python generateImg.py
REM pause
call python Implement_cott_denseCBAM_v1.2.py
REM pause
call python datainsufficient_to_oracle.py
REM pause
exit /B
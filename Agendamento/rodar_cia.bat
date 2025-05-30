@echo off
cd /d "C:\Pedro\Python\email_fh"

:: Adiciona carimbo de data/hora ao log
echo ---------- %date% %time% ---------- >> Agendamento\log_cia.txt

:: Executa o script Python e grava a saída no log
C:\Users\kpm_t\AppData\Local\Programs\Python\Python312\python.exe popular_cia_select.py >> Agendamento\log_cia.txt 2>&1

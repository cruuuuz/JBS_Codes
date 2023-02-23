#######ATUALIZA A ESCALA########

import xlwings as xs
import time

def att():
    wb = xs.Book('O:/Cruz/Relatorios/Escala/Base para escala de abate.xlsm')

    atualiza = wb.macro('Atualiza.Workbook_Open()')
    atualiza()
    wb.save()
    wb.close()
    
    print('ok')
    
    time.sleep(7200)

while True:
    x=1
    att()
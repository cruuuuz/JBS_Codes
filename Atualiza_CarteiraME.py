import xlwings as xs

wb = xs.Book('O:/Cruz/Relatorios/Carteira ME/Acumulado Paises.xlsm')
wb = xs.books.active
wb.save()
wb.close()

print('ok')

wb1 = xs.Book('O:/Cruz/Relatorios/Carteira ME/Acumulado.xlsx')
wb1 = xs.books.active
wb1.save()
wb1.close()

print('ok')
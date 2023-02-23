import shutil
from time import sleep

'''
Fica copiando e colando os arquivos que o CSC gera para a pasta que atualiza o PowerBI
'''

def movefile(source, destionation):
    shutil.copy(source, destionation)


x = 1
while x == 1:
    estoque = movefile('//csc.jbs.com.br/Compartilhado/Planejamento/ftEstoque.txt', 'O:/Cruz/Relatorios/Estoque/ftEstoque.txt')
    carteira = movefile('//csc.jbs.com.br/Compartilhado/Planejamento/FtFollowUp.txt', 'O:/Cruz/Relatorios/Carteira ME/ftFollowUp.txt')
    print('ok')
    sleep(1800)
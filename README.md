# Analise-de-dados-de-sensoriamento-utilizando-pthreads

## COmpilar o programa: 

`
gcc main.c cJSON.c -o sensores -lpthread
`

ou (se usar clock do sistema):

`
gcc main.c cJSON.c -o sensores -lpthread -lrt
`

--> gera o executavel = sensores

## EXECUTAR PROGRAMA

rodar ./sensores

Se tudo estiver correto você verá algo como:

[Produtor] Lendo senzemo_cx_bg.json...
[Produtor] Lendo mqtt_senzemo_cx_bg.json...
[Produtor] Finalizou a leitura de todos os arquivos.
[Consumidor] Estatisticas calculadas com sucesso!

Depois aparecerá o relatório:

============================================================
ANALISE DE DADOS DOS SENSORES - CityLivingLab
Processamento utilizando pthreads
============================================================

Total de registros processados Caxias: 52184
Total de registros processados Bento: 52184

------------------------------------------------------------
TEMPERATURA (C)
------------------------------------------------------------
Cidade            | Minima | Data/Hora             | Maxima | Data/Hora             | Media
-----------------------------------------------------------------------------------------------
Caxias do Sul     | ...
Bento Goncalves   | ...

Verificar o arquivo de log

Seu programa também cria um log:

log_auditoria.txt

Para ver:

cat log_auditoria.txt

ou

nano log_auditoria.txt

###  Se der erro de pthread

Compile assim:

gcc -pthread main.c cJSON.c -o sensores

### Se quiser testar performance

Rode com:

time ./sensores

Ele vai mostrar o tempo também

## APós rodar

nano processamento.log
#!/usr/bin/python3
#./P3_mapper.py < GOOGLE.csv | sort -k 1,1 -t $'\t' | ./P3_reducer.py
import sys

# Leer la primera línea que contiene los encabezados y descartarla
header = next(sys.stdin)

for line in sys.stdin:
        fields = line.strip().split(',')
        
        date = fields[0]
        year = date.split('-')[0]
        
        close_price = fields[4]
        
        print(year + '\t' + close_price)

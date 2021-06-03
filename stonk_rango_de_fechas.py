from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

class StonkRangoDeFechas(MRJob):
    def mapper(self, _, line):
        linea = line.split(',')
        # filtra solo datos del rango pedido
        dia = datetime.datetime.strptime(linea[5], '%Y/%m/%d')
        stonk = linea[0]
        
        # valores buscados
        inicio_rango = datetime.datetime(2021,5,20)
        fin_rango = datetime.datetime(2021,5,30)
        stonk_buscado = "INDITEX"
        if inicio_rango < dia and fin_rango >= dia and stonk_buscado == stonk:
            yield((linea[0]),(linea[5], linea[6], linea[1], linea[2], linea[3]))
        

    def reducer(self, key, values):
        # valores iniciales del mes
        primera_fecha = datetime.datetime(2999,1,1)
        primera_hora = "18"
        val_inicial = 0

        # min , max
        val_min = 9999
        val_max = 0

        # valores finales del mes
        ultima_fecha = datetime.datetime(1999,1,1)
        ultima_hora = "09"
        val_final = 0

        for value in values:
            ultima_cot = float(value[2])
            max_sesion = float(value[3])
            min_sesion = float(value[4])
            fecha = datetime.datetime(int(value[0].split('/')[0]),  int(value[0].split('/')[1]), int(value[0].split('/')[2]) )
            hora = value[1].split('_')[0]
            if fecha <= primera_fecha and int(hora) <= int(primera_hora):
                primera_fecha = fecha
                primera_hora = hora
                val_inicial = ultima_cot
            if fecha >= ultima_fecha and int(hora) >= int(ultima_hora):
                ultima_fecha = fecha
                ultima_hora = hora
                val_final = ultima_cot
            if val_max <= max_sesion:
                val_max = max_sesion
            if val_min >= min_sesion:
                val_min = min_sesion
        if val_min == 0 or val_inicial == 0:
            decremento = 0
        else:
            decremento = (val_min-val_inicial)/val_inicial*100
        if val_max == 0 or val_inicial == 0:
            incremento = 0
        else:
            incremento = (val_max-val_inicial)/val_inicial*100
        
        # (accion), (accion, minimo, maximo, decremento, incremento)
        yield(key ,(val_min, val_max, decremento, incremento))


        
    def reducer_2(self, month, values):
        month_stonks = []
        for value in values:
            month_stonks.append([value[0], value[1], value[2], value[3], value[4]])
        # (primer_dia_del_mes), (list<accion, valor_inicial, valor_final, minimo, maximo>)
        yield(month, month_stonks)



        

if __name__ == '__main__':
    StonkRangoDeFechas.run()


























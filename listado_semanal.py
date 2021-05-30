from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

class ListadoSemanal(MRJob):
    def mapper(self, _, line):
        linea = line.split(',')
        # primer dia de la semana
        dia = linea[5]
        dt = datetime.datetime.strptime(dia, '%Y/%m/%d')
        start = dt - datetime.timedelta(days=dt.weekday())
        first_day = start.strftime('%Y/%m/%d')
        # (accion, primer_dia_de_la_semana), (fecha, hora, ultima_cotizacion, max_sesion, min_sesion)
        yield((linea[0],first_day),(linea[5], linea[6], linea[1], linea[2], linea[3]))

    def reducer(self, key, values):
        # valores iniciales de la semana
        primera_fecha = datetime.datetime(2999,1,1)
        primera_hora = "18"
        val_inicial = 0

        # min , max
        val_min = 0
        val_max = 0

        # valores finales de la semana
        ultima_fecha = datetime.datetime(1999,1,1)
        ultima_hora = "09"
        val_final = 0

        for value in values:
            ultima_cot = float(value[2])
            max_sesion = float(value[3])
            min_sesion = float(value[4])
            fecha = datetime.datetime(int(value[0].split('/')[0]),  int(value[0].split('/')[1]), int(value[0].split('/')[2]))
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
        
        # (primer_dia_de_la_semana), (accion, valor_inicial, valor_final, minimo, maximo)
        yield(key[1] ,(key[0], val_inicial, val_final, val_min, val_max))


        
    def reducer_2(self, week, values):
        week_stonks = []
        for value in values:
            week_stonks.append([value[0], value[1], value[2], value[3], value[4]])
        # (primer_dia_de_la_semana), (list<accion, valor_inicial, valor_final, minimo, maximo>)
        yield(week, week_stonks)



    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                reducer = self.reducer),
            MRStep(reducer = self.reducer_2)
        ]
        

if __name__ == '__main__':
    ListadoSemanal.run()

























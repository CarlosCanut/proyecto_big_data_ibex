from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
from itertools import islice

class StonksByDateAndPercentage(MRJob):
    def mapper(self, _, line):
        linea = line.split(',')
        # primer dia de la semana
        inicio_rango = datetime.datetime(2021,5,25)
        fin_rango = datetime.datetime(2021,6,2)

        stonk = linea[0]
        dia = datetime.datetime.strptime(linea[5], '%Y/%m/%d')

        inicio_rango_str = inicio_rango.strftime('%Y/%m/%d')
        fin_rango_str = fin_rango.strftime('%Y/%m/%d')
        if inicio_rango < dia and fin_rango >= dia:
            # ("range", accion, inicio_rango, fin_rango), (ultima_cotizacion, fecha, hora)
            yield(("range",stonk, inicio_rango_str, fin_rango_str),(linea[1], linea[5], linea[6]))



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
            ultima_cot = float(value[0])
            fecha = datetime.datetime(int(value[1].split('/')[0]),  int(value[1].split('/')[1]), int(value[1].split('/')[2]))
            hora = value[2].split('_')[0]
            if fecha <= primera_fecha and int(hora) <= int(primera_hora):
                primera_fecha = fecha
                primera_hora = hora
                val_inicial = ultima_cot
            if fecha >= ultima_fecha and int(hora) >= int(ultima_hora):
                ultima_fecha = fecha
                ultima_hora = hora
                val_final = ultima_cot
       
        crecimiento = (((val_final - val_inicial)/val_inicial)*100)
        if crecimiento >= 4:
            # ("range", inicio_rango, fin_rango), (accion, crecimiento)
            yield((key[0], key[2], key[3]) ,(key[1], crecimiento))



    # ->
    # ("range", inicio_rango, fin_rango), (accion, crecimiento)
    # ->
    def reducer_2(self, total, values):

        # -----------------------------------------
        # ----------------- TO-DO -----------------
        # -----------------------------------------

        total_stonks = {}
        for stonk in values:
            total_stonks[stonk[0]] = stonk[1]
        sorted_best_total_stonks = dict(sorted(total_stonks.items(), key=lambda item: item[1]))
        # best_stonks = list(sorted_best_total_stonks)[:5]

        best_stonks = {}
        for x in list(reversed(list(sorted_best_total_stonks))):
            best_stonks[x] = sorted_best_total_stonks[x]

        # ("range", inicio_rango, fin_rango), ({"accion": "valor", ... })
        yield(total, best_stonks)


    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                reducer = self.reducer),
            MRStep(reducer = self.reducer_2)
        ]

        

if __name__ == '__main__':
    StonksByDateAndPercentage.run()

























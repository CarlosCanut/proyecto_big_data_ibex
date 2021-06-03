from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
from itertools import islice

class FiveBestStonks(MRJob):
    def mapper(self, _, line):
        linea = line.split(',')
        # primer dia de la semana
        stonk = linea[0]
        dia = linea[5]
        # ultima semana y mes
        today = datetime.datetime.today()
        today_str = today.strftime('%Y/%m/%d')
        dt = datetime.datetime.strptime(today_str, '%Y/%m/%d')

        first_week_day = dt - datetime.timedelta(days=dt.weekday())
        first_month_day = dt.strftime('%Y/%m/01')
        first_week_day = first_week_day.strftime('%Y/%m/%d')


        if first_month_day <= dia:
            # ("month", accion, primer_dia_mes), (ultima_cotizacion, fecha, hora)
            yield(("month",stonk, first_month_day),(linea[1], linea[5], linea[6]))
        if first_week_day <= dia:
            # ("week", accion, primer_dia_semana), (ultima_cotizacion, fecha, hora)
            yield(("week", stonk, first_week_day), (linea[1], linea[5], linea[6]))



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
       
        decrecimiento = val_inicial - val_final
        # ("week", primer_dia_semana), (accion, decrecimiento)
        # ("month", primer_dia_mes), (accion, decrecimiento)
        yield((key[0], key[2]) ,(key[1], decrecimiento))



    # ->
    # ("month", primer_dia_mes), (accion, decrecimiento) | ("week", primer_dia_semana), (accion, decrecimiento)
    # ->
    def reducer_2(self, key, values):

        # -----------------------------------------
        # ----------------- TO-DO -----------------
        # -----------------------------------------

        total_stonks = {}
        for stonk in values:
            total_stonks[stonk[0]] = stonk[1]
        sorted_worst_total_stonks = dict(sorted(total_stonks.items(), key=lambda item: item[1]))

        worst_stonks = {}
        for x in list(reversed(list(sorted_worst_total_stonks)))[:5]:
            worst_stonks[x] = sorted_worst_total_stonks[x]

        # ("week", primer_dia_semana), ({"accion": "valor", ... })
        yield(key, worst_stonks)

        # ("month", primer_dia_mes), ({"accion": "valor", ... })



    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                reducer = self.reducer),
            MRStep(reducer = self.reducer_2)
        ]

        

if __name__ == '__main__':
    FiveBestStonks.run()

























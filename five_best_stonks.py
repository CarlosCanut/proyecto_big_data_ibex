from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

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
        first_month_day = first_week_day.strftime('%Y/%m/01')
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
        
        crecimiento = val_final - val_inicial
        # ("week", primer_dia_semana), (accion, crecimiento)
        # ("month", primer_dia_mes), (accion, crecimiento)
        yield((key[0], key[2]) ,(key[1], crecimiento))



        
    def reducer_2(self, week, values):

        # -----------------------------------------
        # ----------------- TO-DO -----------------
        # -----------------------------------------

        week_stonks.append([value[0], value[1], value[2], value[3], value[4]])
        # ("week", primer_dia_semana), ({"accion": "valor", ... })
        yield(week, week_stonks)

        # ("month", primer_dia_mes), ({"accion": "valor", ... })



    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                reducer = self.reducer)
        ]
        

if __name__ == '__main__':
    FiveBestStonks.run()

























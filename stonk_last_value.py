from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
import os

class StonkLastValue(MRJob):

    def configure_args(self):
        super(StonkLastValue,self).configure_args()
        self.add_passthru_arg('--accion',default='IBERDROLA')

    def mapper(self, _, line):
        linea = line.split(',')
        stonk_requested = self.options.accion
        # primer dia de la semana
        stonk = linea[0]
        dia = linea[5]
        # ultima hora, semana y mes
        today = datetime.datetime.today()
        today_str = today.strftime('%Y/%m/%d')
        now = today.strftime('%Y/%m/%d/%H/%M')
        dt = datetime.datetime.strptime(today_str, '%Y/%m/%d')

        first_week_day = dt - datetime.timedelta(days=dt.weekday())
        first_month_day = dt.strftime('%Y/%m/01')
        first_week_day = first_week_day.strftime('%Y/%m/%d')
        one_hour_ago = today - datetime.timedelta(hours=1)
        one_hour_ago = one_hour_ago.strftime('%Y/%m/%d/%H/%M')
        hora_de_registro = dia + linea[6].split('_')[0] + "/" + linea[6].split('_')[1]


        if stonk_requested == stonk and first_month_day <= dia:
            # (accion, primer_dia_del_mes), (fecha, hora, ultima_cotizacion, max_sesion, min_sesion)
            yield((stonk, first_month_day),(linea[5], linea[6], linea[1], linea[2], linea[3]))
        if stonk_requested == stonk and first_week_day <= dia:
            # (accion, primer_dia_de_la_semana), (fecha, hora, ultima_cotizacion, max_sesion, min_sesion)
            yield((stonk, first_week_day), (linea[5], linea[6], linea[1], linea[2], linea[3]))
        if stonk_requested == stonk and hora_de_registro > one_hour_ago:
            # (accion, ultima_hora), (fecha, hora, ultima_cotizacion, max_sesion, min_sesion)
            yield((stonk, one_hour_ago), (linea[5], linea[6], linea[1], linea[2], linea[3]))

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
    StonkLastValue.run()

























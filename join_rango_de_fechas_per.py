from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

class JoinStonkRangoDeFechas(MRJob):

    def configure_args(self):
        super(JoinStonkRangoDeFechas,self).configure_args()
        self.add_passthru_arg('--accion',default='IBERDROLA')
        self.add_passthru_arg('--inicio', default='2021/05/26')
        self.add_passthru_arg('--fin', default='2021/06/04')

    def mapper(self, _, line):
        linea = line.split(',')
        stonk_buscado = self.options.accion
        # PER y BPA
        if linea[0] == "BPA_PER":
            if linea[1] == stonk_buscado:
                # (accion), ("BPA_PER",per_2020,bpa_2020,bpa_2019,bpa_2018)
                yield((linea[1]),(linea[0],linea[11],linea[10],linea[9],linea[8]))
        else:
            # filtra solo datos del rango pedido
            dia = datetime.datetime.strptime(linea[5], '%Y/%m/%d')
            stonk = linea[0]
            
            # valores buscados
            inicio_rango = datetime.datetime.strptime(self.options.inicio, '%Y/%m/%d')
            fin_rango = datetime.datetime.strptime(self.options.fin, '%Y/%m/%d')
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
            if value[0] == "BPA_PER":
                per_2020 = value[1]
                bpa_2020 = value[2]
                bpa_2019 = value[3]
                bpa_2018 = value[4]
            else:
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
            decremento = (((val_min-val_inicial)/val_inicial)*100)
        if val_max == 0 or val_inicial == 0:
            incremento = 0
        else:
            incremento = (((val_max-val_inicial)/val_inicial)*100)
        
        # (accion), (minimo, maximo, decremento, incremento, per_2020, bpa_2020, bpa_2019, bpa_2018)
        yield(key ,(val_min, val_max, decremento, incremento, per_2020, bpa_2020, bpa_2019, bpa_2018))


        
    def reducer_2(self, key, values):
        month_stonks = []
        for value in values:
            month_stonks.append([value[0], value[1], value[2], value[3]])
        # (primer_dia_del_mes), (list<valor_inicial, valor_final, minimo, maximo>)
        yield(key, month_stonks)




        

if __name__ == '__main__':
    JoinStonkRangoDeFechas.run()


























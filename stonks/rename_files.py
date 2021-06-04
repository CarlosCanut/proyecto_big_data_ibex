import pandas as pd
import datetime

weekDays = ("Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday")

def save_file(file_name):
    today = datetime.date.today()
    today = today.strftime('%Y-%m-%d')
    dt = datetime.datetime.strptime(today, '%Y-%m-%d')
    # primer dia de la semana
    start = dt - datetime.timedelta(days=dt.weekday())
    first_day = start.strftime('%d')
    month = start.strftime('%m')
    year = start.strftime('%Y')
    day_name = weekDays[start.weekday()]


# open files
year = "2021"

for day in ['04_21', '05_18', '05_19', '05_20', '05_21', '05_25', '05_26', '05_27', '05_28', '05_31', '06_01', '06_02', '06_03', '06_04']:
    day_df = pd.DataFrame(columns=['accion', 'ultima_cotizacion', 'max_sesion', 'min_sesion', 'ultima_actualizacion', 'fecha', 'hora'])
    for hour in ["09_30", "10_30", "11_30", "12_30", "13_30", "14_30", "15_30", "16_30", "17_30", "18_30"]:
        try:
            #print("-----------------------------------------------------------------")
            #print(hour)
            hour_df = pd.read_csv("temp/reduced_"+ year + "_" + day + "_" + hour + ".csv")
            hour_df.columns = ['accion', 'ultima_cotizacion', 'max_sesion', 'min_sesion', 'ultima_actualizacion']
            hour_df['fecha'] = str(year+"/"+ str(day.split('_')[0]) + "/" + str(day.split('_')[1]) )
            hour_df['hora'] = str(hour)
            #print(hour_df)
            day_df = day_df.append(hour_df, ignore_index=True)
        except:
            print("File not found: ", "reduced_"+ year + "_" + day + "_" + hour + ".csv")
            pass

    day_df.to_csv(year+"_" + day + ".csv", index=False, header=False)





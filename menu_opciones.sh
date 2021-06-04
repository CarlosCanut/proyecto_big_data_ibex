#!/bin/bash
start-dfs.sh
start-yarn.sh
git pull


if hdfs dfs -test -e /stonks
then
    echo ""
else
    hdfs dfs -mkdir /stonks
fi
if hdfs dfs -test -e /stonks/resultados
then
    echo ""
else
    hdfs dfs -mkdir /stonks/resultados
fi

cmd=(dialog --separate-output --print-maxsize --checklist "Select options:" 44 100 36)
options=(1 "Actualizar los datos de hdfs." off
	 2 "Generar un listado semanal donde se indique, para cada acción, su valor inicial, final, mínimo y máximo." off    
         3 "Generar un listado mensual donde se indique, para cada acción, su valor inicial, final, mínimo y máximo." off
         4 "Dado el nombre de una acción y un rango de fechas, obtener su valor mínimo y máximo de cotización, \ así como el porcentaje de decremento y de incremento desde el valor inicial de cotización hasta el mínimo y máximo, respectivamente." off
         5 "Dado el nombre de una acción, recuperar su valor mínimo y máximo de cotización de la última hora, semana y mes." off
	 6 "Mostrar las 5 acciones que más han subido en la última semana y último mes." off
	 7 "Mostrar las 5 acciones que más han bajado en la última semana y último mes." off 
	 8 "Dado un porcentaje y un rango de fechas, mostrar las acciones que han tenido un incremento de este porcentaje durante este período." off)
choices=$("${cmd[@]}" "${options[@]}" 2>&1 >/dev/tty)
clear
for choice in $choices
do
    case $choice in
        1)
	    echo ""
	    echo "		###################################################"
	    echo "		##########  - Actualizar datos en hdfs: ###########"
	    echo "		###################################################"
	    echo ""
	    local_stonks=$(ls -p stonks | grep -v / | grep .csv)
	    ORANGE='\033[1;31m'
	    BLUE='\033[1;34m'
	    NC='\033[0m'
	    for stonk in $local_stonks; do
		if hdfs dfs -test -e /stonks/$stonk
		then
			echo -e "El documento ${ORANGE} [$stonk] ${NC} ya existe"
		else
			hdfs dfs -put stonks/$stonk /stonks/$stonk
			echo -e "+ documento ${BLUE} [$stonk] ${NC} añadido +"
		fi
	     done
	    echo ""
            ;;
        2)
	    echo ""
	    echo "		###################################################"
	    echo "		########  - Listado semanal por acciones: #########"
	    echo "		###################################################"
	    echo ""
	    ./lanzar_listado.sh -a "listado_semanal.py" -o "/resultados/listado_semanal"
	    clear
	    echo ""
	    echo "		###################################################"
	    echo "		########  - Listado semanal por acciones: #########"
	    echo "		###################################################"
	    echo ""
	    echo $(hdfs dfs -cat /resultados/listado_semanal/part*) 
            ;;
        3)
	    echo ""
	    echo "		###################################################"
	    echo "		########  - Listado mensual por acciones: #########"
	    echo "		###################################################"
	    echo ""
	    ./lanzar_listado.sh -a "listado_mensual.py" -o "/resultados/listado_mensual"
	    clear
	    echo ""
	    echo "		###################################################"
	    echo "		########  - Listado mensual por acciones: #########"
	    echo "		###################################################"
	    echo ""
	    echo $(hdfs dfs -cat /resultados/listado_mensual/part*) 
            ;;
        4)
	    echo ""
	    echo ""
	    echo "		########################################################################"
	    echo "		########  - Incremento/Decremento de acción y rango de fechas: #########"
	    echo "		########################################################################"
	    echo ""
	    read -p "Introduce el nombre de la accion a analizar:   `echo $'\n> '`" stonk_4
	    read -p "El inicio del rango de fechas con formato %Y%m%d (ej. 20210526): `echo $'\n> '`" inicio_rango_4
	    read -p "El final del rango de fechas con formato %Y%m%d (ej. 20210526): `echo $'\n> '`" fin_rango_4
	    ./lanzar_script_rango_dias.sh -s $inicio_rango_4 -e $fin_rango_4 -a ".py" -o "/resultados/incremento_decremento_accion"
            echo "Los datos son: "
	    echo $stonk_4
	    echo $inicio_rango_4 " - " $fin_rango_4
            ;;
        5)
	    echo ""
	    echo ""
	    echo "		####################################################"
	    echo "		########  - mínimo y máximo de una acción: #########"
	    echo "		####################################################"
	    echo ""
	    read -p "Introduce el nombre de la accion a analizar:   `echo $'\n> '`" stonk_5
	    fecha_inicio=$(date --date="1 month ago" +"%Y%m%d")
	    fecha_final=$(date --date="today" +"%Y%m%d")
	    ./lanzar_script_accion.sh -s $fecha_inicio -e $fecha_final -a "stonk_last_value.py" -o "/resultados/min_max_ultima_hora_semana_mes" -k "IBERDROLA"
	    clear
	    echo ""
	    echo "		####################################################"
	    echo "		########  - mínimo y máximo de una acción: #########"
	    echo "		####################################################"
	    echo ""
	    echo $(hdfs dfs -cat /resultados/min_max_ultima_hora_semana_mes/part*) 
            ;;
        6)
	    echo ""
	    echo "		###################################################"
	    echo "		#### - Las 5 acciones que más han subido son: #####"
	    echo "		###################################################"
	    echo ""
	    fecha_inicio=$(date --date="1 month ago" +"%Y%m%d")
	    fecha_final=$(date --date="today" +"%Y%m%d")
	    ./lanzar_script_rango_dias.sh -s $fecha_inicio -e $fecha_final -a "five_best_stonks.py" -o "/resultados/cinco_acciones_con_mas_subida"

            ;;
        7)
	    echo ""
	    echo "		###################################################"
	    echo "		#### - Las 5 acciones que más han bajado son: #####"
	    echo "		###################################################"
	    echo ""
	    fecha_inicio=$(date --date="1 month ago" +"%Y%m%d")
	    fecha_final=$(date --date="today" +"%Y%m%d")
	    ./lanzar_script_rango_dias.sh -s $fecha_inicio -e $fecha_final -a "five_worst_stonks.py" -o "/resultados/cinco_acciones_con_mas_bajada"
            ;;
        8)
	    echo ""
	    echo ""
	    echo "		#############################################################################"
	    echo "		#### - Acciones con un porcentaje de incremento indicado en un periodo: #####"
	    echo "		#############################################################################"
	    echo ""
	    read -p "Introduce el porcentaje a analizar:   `echo $'\n> '`" stonk_8
	    read -p "El inicio del rango de fechas con formato %Y/%m/%d (ej. 2021/5/26): `echo $'\n> '`" inicio_rango_8
	    read -p "El final del rango de fechas con formato %Y/%m/%d (ej. 2021/5/26): `echo $'\n> '`" fin_rango_8
	    echo $inicio_rango_8 " - " $fin_rango_8
            ;;
    esac
done


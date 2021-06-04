#!/bin/bash
start-dfs.sh
start-yarn.sh
git pull

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
	    hdfs dfs -put stonks/ /stonks/
	    echo ""
            ;;
        2)
	    echo ""
	    echo "		###################################################"
	    echo "		########  - Listado semanal por acciones: #########"
	    echo "		###################################################"
	    echo ""
	    python listado_semanal.py stonks/*.csv 
            ;;
        3)
	    echo ""
	    echo "		###################################################"
	    echo "		########  - Listado mensual por acciones: #########"
	    echo "		###################################################"
	    echo ""
	    python listado_mensual.py stonks/*.csv 
            ;;
        4)
	    echo ""
	    read -p "Introduce el nombre de la accion a analizar:   `echo $'\n> '`" stonk_4
	    read -p "El inicio del rango de fechas con formato %Y/%m/%d (ej. 2021/5/26): `echo $'\n> '`" inicio_rango_4
	    read -p "El final del rango de fechas con formato %Y/%m/%d (ej. 2021/5/26): `echo $'\n> '`" fin_rango_4
            echo "Los datos son: "
	    echo $stonk_4
	    echo $inicio_rango_4 " - " $fin_rango_4
            ;;
        5)
	    echo ""
	    read -p "Introduce el nombre de la accion a analizar:   `echo $'\n> '`" stonk_5
	    echo $stonk_5
            ;;
        6)
	    echo ""
	    echo "		###################################################"
	    echo "		#### - Las 5 acciones que más han subido son: #####"
	    echo "		###################################################"
	    echo ""
	    python five_best_stonks.py stonks/*.csv
            ;;
        7)
	    echo ""
	    echo "		###################################################"
	    echo "		#### - Las 5 acciones que más han bajado son: #####"
	    echo "		###################################################"
	    echo ""
	    python five_worst_stonks.py stonks/*.csv
            ;;
        8)
	    echo ""
	    read -p "Introduce el porcentaje a analizar:   `echo $'\n> '`" stonk_8
	    read -p "El inicio del rango de fechas con formato %Y/%m/%d (ej. 2021/5/26): `echo $'\n> '`" inicio_rango_8
	    read -p "El final del rango de fechas con formato %Y/%m/%d (ej. 2021/5/26): `echo $'\n> '`" fin_rango_8
	    echo $inicio_rango_8 " - " $fin_rango_8
            ;;
    esac
done


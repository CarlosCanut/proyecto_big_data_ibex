
read -p "¿Que accion quieres analizar? " stonk_name
read -p "¿Fecha de inicio del informe? " start_date
read -p "¿Fecha de fin del informe?" end_date

outpath="test.csv"
> $outpath
d=
n=0
start=$(date -d "$start_date" +"%Y%m%d")
end=$(date -d "$end_date" +"%Y%m%d")
if [ $end -ge $start ]
then
	until [ "$d" = "$end_date" ]
	do
		((n++))
		d=$(date -d "$start_date + $n days" +"%Y%m%d")
		file_path=$(date -d "$start_date + $n days" +"%Y_%m_%d")
		for hour in 09_30 10_30 11_30 12_30 13_30 14_30 15_30 16_30 17_30 18_30 ; do
			file="reduced_"$file_path"_"$hour".csv"
			if hdfs dfs -test -e /stonks/$file
			then
				echo $file >> $outpath
			else
				echo "file not found :("
			fi
		done
	done
else
	echo "La fecha de inicio debe ser menor que la fecha final"
fi

final_file=$(cat $outpath | wc -l)
if [ $final_file == 0 ]
then
	echo "no se han encontrado registros de este rango de fechas :("
else
	cat $outpath
	# enviar a mapReduce cada fichero listado en cat $outpath
fi


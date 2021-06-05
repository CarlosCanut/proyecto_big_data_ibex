
read -p "¿Quieres hacer un informe semanal o mensual? (1: semanal, 2: mensual) " report_type

if [ $report_type == 1 ]
then
	read -p "¿Cuantas semanas quieres en el listado? " n_semanas
	n_meses=0
else
	read -p "¿Cuantos meses quieres en el listado? " n_meses
	n_semanas=3
fi

# file with requested records path
output_date=$(date +"%Y_%m_%d")
output_path="temp/"$output_date".csv"
> $output_path

for month in $(seq 0 $((n_meses))); do
	echo month: $month
	for week in $(seq 0 $((n_semanas))); do

		echo --------
		echo week: $week
		new_date=$(date --date="$week weeks ago" +"%Y/%m/%d")
		echo $new_date


		# --------- monday ----------
		monday=$(date -d "last monday -$week weeks" +"%Y/%m/%d")
		echo monday: $monday
		flag=0
		for x in $(ls stonks/ | cat | awk -F"_" '{print$2"/"$3"/"$4}'); do
			if [ $monday == $x ] && [ $flag == 0 ]
			then	
				day=($(echo $x | tr "/" "\n"))
				for hour in {"09_30","10_30","11_30","12_30","13_30","14_30","15_30","16_30","17_30","18_30"}; do
					if hdfs dfs -test -e /stonks/reduced_${day[0]}_${day[1]}_${day[2]}_$hour.csv
					then
						echo "stonks/reduced_"${day[0]}"_"${day[1]}"_"${day[2]}"_"$hour".csv" >> $output_path
					fi
				done	
				let flag=flag+1
			fi
		done	

		
		# --------- tuesday ----------
		tuesday=$(date -d "last tuesday -$week weeks" +"%Y/%m/%d")
		echo tuesday: $tuesday
		flag=0
		for x in $(ls stonks/ | cat | awk -F"_" '{print$2"/"$3"/"$4}'); do
			if [ $tuesday == $x ] && [ $flag == 0 ]
			then
				day=($(echo $x | tr "/" "\n"))
				for hour in {"09_30","10_30","11_30","12_30","13_30","14_30","15_30","16_30","17_30","18_30"}; do
					if hdfs dfs -test -e /stonks/reduced_${day[0]}_${day[1]}_${day[2]}_$hour.csv
					then
						echo "stonks/reduced_"${day[0]}"_"${day[1]}"_"${day[2]}"_"$hour".csv" >> $output_path
					fi
				done	
				
				let flag=flag+1
			fi
		done	

		
		# --------- wednesday ----------
		wednesday=$(date -d "last wednesday -$week weeks" +"%Y/%m/%d")
		echo wednesday: $wednesday
		flag=0
		for x in $(ls stonks/ | cat | awk -F"_" '{print$2"/"$3"/"$4}'); do
			if [ $wednesday == $x ] && [ $flag == 0 ]
			then
				day=($(echo $x | tr "/" "\n"))
				for hour in {"09_30","10_30","11_30","12_30","13_30","14_30","15_30","16_30","17_30","18_30"}; do
					if hdfs dfs -test -e /stonks/reduced_${day[0]}_${day[1]}_${day[2]}_$hour.csv
					then
						echo "stonks/reduced_"${day[0]}"_"${day[1]}"_"${day[2]}"_"$hour".csv" >> $output_path
					fi
				done	
				let flag=flag+1
			fi
		done	


		# --------- thursday ----------
		thursday=$(date -d "last thursday -$week weeks" +"%Y/%m/%d")
		echo thursday: $thursday
		flag=0
		for x in $(ls stonks/ | cat | awk -F"_" '{print$2"/"$3"/"$4}'); do
			if [ $thursday == $x ] && [ $flag == 0 ]
			then
				day=($(echo $x | tr "/" "\n"))
				for hour in {"09_30","10_30","11_30","12_30","13_30","14_30","15_30","16_30","17_30","18_30"}; do
					if hdfs dfs -test -e /stonks/reduced_${day[0]}_${day[1]}_${day[2]}_$hour.csv
					then
						echo "stonks/reduced_"${day[0]}"_"${day[1]}"_"${day[2]}"_"$hour".csv" >> $output_path
					fi
				done	
				let flag=flag+1
			fi
		done	


		# --------- friday ----------
		friday=$(date -d "last friday -$week weeks" +"%Y/%m/%d")
		echo friday: $friday
		flag=0
		for x in $(ls stonks/ | cat | awk -F"_" '{print$2"/"$3"/"$4}'); do
			if [ $friday == $x ] && [ $flag == 0 ]
			then
				day=($(echo $x | tr "/" "\n"))
				for hour in {"09_30","10_30","11_30","12_30","13_30","14_30","15_30","16_30","17_30","18_30"}; do
					if hdfs dfs -test -e /stonks/reduced_${day[0]}_${day[1]}_${day[2]}_$hour.csv
					then
						echo "stonks/reduced_"${day[0]}"_"${day[1]}"_"${day[2]}"_"$hour".csv" >> $output_path
					fi
				done	
				let flag=flag+1
			fi
		done	

		echo --------

	done
done


cat $output_path
# enviar a mapReduce cada fichero listado en $output_path



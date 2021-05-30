
read -p "¿Que accion quieres analizar? " stonk_name

outpath="test.csv"
> $outpath

# para la ultima semana copiar lo hecho en el fichero de ultima semana
# para el ultimo mes copiar lo hecho en el fichero de ultimo mes

today=$(date +"%Y_%m_%d")
week_day=$(date +"%u")
hour=$(date +"%H")
minute=$(date +"%M")

####################### Última hora ######################
if [ $week_day == 6 ] | [ $week_day == 7 ]; then
	last_friday=$(date --date="last Friday" +"%Y_%m_%d")
	last_hour=$last_friday"_18_30.csv"
else
	for x in 09 10 11 12 13 14 15 16 17 18; do
		if [ $hour -ge $x ] && [ $minute -ge 30 ]
		then
			last_hour=$($today"_"$x"_30.csv")
		fi
	done
fi

echo $last_hour

# Pasar a mapReduce el fichero con los datos de la última hora "reduced_"$last_hour y la acción $stonk_name y el dato que devuelva en el outfile los datos requeridos en un nuevo fichero


####################### Última semana ######################
last_monday=$(date --date="last Friday - 4 days" +"%Y_%m_%d")
last_tuesday=$(date --date="last Friday - 3 days" +"%Y_%m_%d")
last_wednesday=$(date --date="last Friday - 2 days" +"%Y_%m_%d")
last_thursday=$(date --date="last Friday - 1 days" +"%Y_%m_%d")
last_friday=$(date --date="last Friday" +"%Y_%m_%d")
for x in 09_30 10_30 11_30 12_30 13_30 14_30 15_30 16_30 17_30 18_30; do
	$last_monday"_"$x >> $last_week_outfile
	$last_tuesday"_"$x >> $last_week_outfile
	$last_wednesday"_"$x >> $last_week_outfile
	$last_thursday"_"$x >> $last_week_outfile
	$last_friday"_"$x >> $last_week_outfile
done

# Pasar a mapReduce el fichero con los datos de la última hora "reduced_"$last_week_outfile y la acción $stonk_name y el dato que devuelva en el outfile los datos requeridos en un nuevo file





####################### Último mes ######################
first_day_current_month=$(date -d "-0 month -$(($(date +%d)-1)) days" +"%Y_%m_%d")
first_day_previous_month=$(date -d "-1 month -$(($(date +%d)-1)) days" +"%Y_%m_%d")
last_day_current_month=$(date -d "-$(date +%d) days +1 month" +"%Y_%m_%d")
last_day_last_month=$(date -d "-$(date +%d) days -0 month" +"%Y_%m_%d")
last_day_month_before_last_month=$(date -d "-$(date +%d) days -1 month" +"%Y_%m_%d")


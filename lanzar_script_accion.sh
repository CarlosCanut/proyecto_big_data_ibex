
while getopts ":s:e:a:o:k:" opt; do
  case $opt in
    s) start_date="$OPTARG"
    ;;
    e) end_date="$OPTARG"
    ;;
    a) script_name="$OPTARG"
    ;;
    o) output_path="$OPTARG"
    ;;
    k) accion="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done



d=
n=0
start=$(date -d "$start_date" +"%Y%m%d")
end=$(date -d "$end_date" +"%Y%m%d")
path_list=""
if [ $end -ge $start ]
then
        until [ "$d" == "$end_date" ]
        do
                d=$(date -d "$start_date + $n days" +"%Y%m%d")
                file_path=$(date -d "$start_date + $n days" +"%Y_%m_%d")
		file=$file_path".csv"
		echo ""
		echo $d
		if hdfs dfs -test -e /stonks/$file
		then
			path_list="${path_list} hdfs:///stonks/$file"
		else
			echo ""
		fi
                ((n++))
        done
else
        echo "La fecha de inicio debe ser menor que la fecha final"
fi


python $script_name -r hadoop $path_list --accion $accion --output-dir $output_path

